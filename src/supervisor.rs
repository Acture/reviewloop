//! The machine-level supervisor: one long-running process per state directory
//! runs every explicitly enabled project's triggers, review queue and lease
//! recovery through one fair, machine-wide provider budget
//! ([`Scheduler`]).
//!
//! - **Enabled projects only.** A registered project is ignored until
//!   `project enable`; a disabled one is ignored entirely (no triggers,
//!   submissions, polls, timeouts or recovery) while its jobs keep their
//!   state for explicit CLI commands.
//! - **One per state directory.** [`SupervisorLock`] holds an OS lock on
//!   `<state_dir>/supervisor.lock` for the process lifetime; the kernel drops
//!   it when the process dies, so a restart never waits for a stale lock.
//! - **Pause** is a persistent flag in the database: a paused supervisor
//!   keeps its heartbeat but runs no triggers and contacts no provider, and a
//!   restart stays paused. Pause, resume, enable and disable wake a sleeping
//!   supervisor within [`CONTROL_POLL`].
//! - **Config changes** apply on the next tick: the global config and every
//!   enabled project's `reviewloop.toml` are reloaded each tick. A project
//!   whose config no longer loads is skipped and reports the error in its
//!   health; the others keep running. A global config that moves the
//!   database or state directory stops the supervisor, so its service
//!   manager restarts it on the new paths.

use crate::{
    config::{Config, MachineConfig, canonical_config_path},
    db::Db,
    model::{RegisteredProject, SupervisorRecord},
    notifier::NotificationKind,
    panel,
    registry::{self, Registration, RegistryConflict},
    widget_state,
    worker::{BackendFactory, RoundRobin, Scheduler, TickBudget, TickReport, fire_notification},
};
use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Duration, Utc};
use serde::Serialize;
use serde_json::json;
use std::{
    collections::HashMap,
    fs::{File, OpenOptions, TryLockError},
    future::Future,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration as StdDuration,
};
use tokio::sync::Notify;
use tracing::{error, info, warn};

/// Time between the end of one tick and the start of the next.
pub const TICK_INTERVAL: StdDuration = StdDuration::from_secs(30);
const HEARTBEAT_INTERVAL: StdDuration = StdDuration::from_secs(10);
/// Without a heartbeat for this long, a supervisor is presumed gone.
pub const HEARTBEAT_STALE_AFTER: Duration = Duration::seconds(60);
/// How often a sleeping supervisor checks for pause, resume, enable and
/// disable.
pub const CONTROL_POLL: StdDuration = StdDuration::from_secs(2);
const LOCK_FILE: &str = "supervisor.lock";

/// The supervisor as others see it, from the control row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SupervisorState {
    /// Alive and working.
    Running,
    /// Alive, but contacts no provider and runs no triggers.
    Paused,
    /// Not running (never started, stopped, or its heartbeat went stale).
    Stopped,
}

impl SupervisorState {
    pub fn of(record: &SupervisorRecord, now: DateTime<Utc>) -> Self {
        let alive = record.stopped_at.is_none()
            && record
                .heartbeat_at
                .is_some_and(|beat| now - beat <= HEARTBEAT_STALE_AFTER);
        match (alive, record.paused_at.is_some()) {
            (false, _) => Self::Stopped,
            (true, true) => Self::Paused,
            (true, false) => Self::Running,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Stopped => "stopped",
        }
    }
}

/// One registered project as the supervisor last left it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectState {
    /// The supervisor ignores it.
    Disabled,
    /// Enabled, but no supervisor has run it yet.
    Pending,
    /// The last pass had no errors.
    Ok,
    /// The last pass failed; see `last_error`.
    Error,
}

impl ProjectState {
    pub fn of(project: &RegisteredProject) -> Self {
        if !project.enabled {
            Self::Disabled
        } else if project.health.last_error.is_some() {
            Self::Error
        } else if project.health.last_run_at.is_some() {
            Self::Ok
        } else {
            Self::Pending
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Pending => "pending",
            Self::Ok => "ok",
            Self::Error => "error",
        }
    }
}

/// Exclusive right to supervise one state directory, held until dropped
/// (or the process dies).
#[derive(Debug)]
pub struct SupervisorLock {
    _file: File,
}

impl SupervisorLock {
    pub fn path(state_dir: &Path) -> PathBuf {
        state_dir.join(LOCK_FILE)
    }

    /// Take the lock, or fail naming the supervisor that holds it.
    pub fn acquire(state_dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(state_dir)
            .with_context(|| format!("failed to create state dir {}", state_dir.display()))?;
        let path = Self::path(state_dir);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .with_context(|| format!("failed to open {}", path.display()))?;
        match file.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                let holder = std::fs::read_to_string(&path).unwrap_or_default();
                let holder = holder.trim();
                return Err(anyhow!(
                    "another reviewloop supervisor is already running for state dir {} ({}); \
                     stop it first, or check it with `reviewloop daemon status`",
                    state_dir.display(),
                    if holder.is_empty() {
                        "pid unknown"
                    } else {
                        holder
                    }
                ));
            }
            Err(TryLockError::Error(err)) => {
                return Err(err).with_context(|| format!("failed to lock {}", path.display()));
            }
        }
        file.set_len(0)?;
        writeln!(file, "pid {}", std::process::id())?;
        file.sync_all()?;
        Ok(Self { _file: file })
    }
}

/// The enabled projects whose configs loaded this tick, and those that did
/// not (with the reason).
#[derive(Debug, Default)]
pub struct EnabledProjects {
    pub loaded: Vec<Config>,
    pub failed: Vec<(String, String)>,
}

/// Load every enabled project's config on `machine`'s settings.
pub fn load_enabled_projects(db: &Db, machine: &MachineConfig) -> Result<EnabledProjects> {
    let mut projects = EnabledProjects::default();
    for row in db
        .list_registered_projects()?
        .into_iter()
        .filter(|row| row.enabled)
    {
        match load_enabled_project(machine, &row) {
            Ok(config) => projects.loaded.push(config),
            Err(err) => projects.failed.push((row.project_id, format!("{err:#}"))),
        }
    }
    Ok(projects)
}

fn load_enabled_project(machine: &MachineConfig, row: &RegisteredProject) -> Result<Config> {
    let path = canonical_config_path(&row.config_path).with_context(|| {
        format!(
            "config of enabled project {} is gone; enable the project from its new location with \
             `reviewloop project enable`, or disable it with `reviewloop project disable --project-id {}`",
            row.project_id, row.project_id
        )
    })?;
    let config = machine.project(&path)?;
    if config.project_id != row.project_id {
        return Err(anyhow!(
            "{} now declares project {:?}, not {}; enable it under its new id with \
             `reviewloop project enable`, and disable this one with \
             `reviewloop project disable --project-id {}`",
            path.display(),
            config.project_id,
            row.project_id,
            row.project_id
        ));
    }
    config.validate_for_foreign_load()?;
    Ok(config)
}

/// What became of the project a single-project daemon install was bound to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LegacyAdoption {
    /// Nobody had decided yet: it is enabled now and keeps running.
    Enabled,
    /// Someone already enabled or disabled it; that decision stands.
    AlreadyDecided { enabled: bool },
    /// Another live config holds its registration; nothing was enabled.
    Conflict(RegistryConflict),
}

/// Keep the project an old `daemon install --config <path>` bound the daemon
/// to running under the supervisor: register it and enable it, unless
/// someone already decided otherwise. Other registered projects stay as
/// they are. Returns the project id with the outcome.
pub fn adopt_legacy_binding(
    db: &Db,
    machine: &MachineConfig,
    config_path: &Path,
    now: DateTime<Utc>,
) -> Result<(String, LegacyAdoption)> {
    let path = canonical_config_path(config_path)?;
    let config = machine.project(&path).with_context(|| {
        format!(
            "failed to load the project the daemon install was bound to ({})",
            path.display()
        )
    })?;
    let project_id = config.project_id;
    if let Registration::Kept(conflict) = registry::register_seen(db, &project_id, &path, now)? {
        return Ok((project_id, LegacyAdoption::Conflict(conflict)));
    }
    if db.enable_undecided_project(&project_id, now)? {
        return Ok((project_id, LegacyAdoption::Enabled));
    }
    let enabled = db
        .get_registered_project(&project_id)?
        .is_some_and(|row| row.enabled);
    Ok((project_id, LegacyAdoption::AlreadyDecided { enabled }))
}

/// What one supervisor tick did.
#[derive(Debug)]
pub enum TickOutcome {
    /// Paused: nothing ran.
    Paused,
    Ran(TickReport),
}

/// What a supervisor remembers between ticks: whose turn it is, and the
/// errors it already reported (notifications fire on changes only).
#[derive(Debug, Default)]
pub struct SupervisorMemory {
    turns: RoundRobin,
    project_errors: HashMap<String, String>,
    machine_error: Option<String>,
}

/// The global config moved the database or state directory away from the
/// ones this supervisor holds.
#[derive(Debug, thiserror::Error)]
#[error(
    "the global config now uses state dir {new_state_dir} and database {new_db}; this supervisor \
     holds {old_state_dir} and {old_db}. Exiting so the service manager restarts it on the new paths"
)]
pub struct StorageMoved {
    pub old_state_dir: String,
    pub new_state_dir: String,
    pub old_db: String,
    pub new_db: String,
}

/// The supervisor of one machine.
pub struct Supervisor<'a> {
    pub db: &'a Db,
    pub backends: &'a dyn BackendFactory,
    /// Reloaded at every tick, so config changes apply without a restart.
    pub load_machine: &'a dyn Fn() -> Result<MachineConfig>,
    pub interval: StdDuration,
    pub panel: bool,
}

impl Supervisor<'_> {
    /// Supervise until `shutdown` resolves (the tick in progress finishes
    /// its provider call first). Fails at once when another supervisor holds
    /// this state dir, or another live one supervises this database.
    pub async fn run(&self, shutdown: impl Future<Output = ()> + Send + 'static) -> Result<()> {
        let machine = (self.load_machine)()?;
        let state_dir = machine.config.state_dir();
        let _lock = SupervisorLock::acquire(&state_dir)?;
        let pid = std::process::id();
        let record = self.db.supervisor_record()?;
        if SupervisorState::of(&record, Utc::now()) != SupervisorState::Stopped
            && record.state_dir.as_deref() != Some(state_dir.as_path())
        {
            return Err(anyhow!(
                "database {} is already supervised from state dir {} (pid {}); one database takes \
                 one supervisor, so point both at the same state dir or stop the other one",
                self.db.path.display(),
                record
                    .state_dir
                    .as_deref()
                    .map_or_else(|| "unknown".to_string(), |dir| dir.display().to_string()),
                record
                    .pid
                    .map_or_else(|| "unknown".to_string(), |pid| pid.to_string())
            ));
        }
        self.db
            .record_supervisor_start(pid, &state_dir, env!("CARGO_PKG_VERSION"), Utc::now())?;
        info!(pid, state_dir = %state_dir.display(), db = %self.db.path.display(), "supervisor started");

        let stopping = Arc::new(AtomicBool::new(false));
        let wake = Arc::new(Notify::new());
        let watcher = {
            let stopping = Arc::clone(&stopping);
            let wake = Arc::clone(&wake);
            tokio::spawn(async move {
                shutdown.await;
                stopping.store(true, Ordering::SeqCst);
                wake.notify_one();
            })
        };
        let heartbeat = tokio::spawn(heartbeat(self.db.reopen()?, pid));

        let mut memory = SupervisorMemory::default();
        let mut number = 0;
        let mut result = Ok(());
        while !stopping.load(Ordering::SeqCst) {
            number += 1;
            let control = self.control_version();
            match self.tick_once(pid, number, &mut memory, &stopping).await {
                Ok(_) => {}
                Err(err) if err.is::<StorageMoved>() => {
                    error!(error = %format!("{err:#}"), "supervisor stopping");
                    result = Err(err);
                    break;
                }
                Err(err) => self.machine_failure(pid, &err, &mut memory),
            }
            self.wait(control, &stopping, &wake).await;
        }

        heartbeat.abort();
        watcher.abort();
        if let Err(err) = self.db.record_supervisor_stop(pid, Utc::now()) {
            warn!(error = %err, "failed to record the supervisor stop");
        }
        info!(pid, "supervisor stopped");
        result
    }

    /// One tick: reload the machine config, then, unless paused, run every
    /// enabled project and record its health. `Err` is a machine-level
    /// failure (no project ran).
    pub async fn tick_once(
        &self,
        pid: u32,
        number: u64,
        memory: &mut SupervisorMemory,
        stopping: &AtomicBool,
    ) -> Result<TickOutcome> {
        let machine = (self.load_machine)().context("loading the global config")?;
        self.ensure_same_storage(&machine)?;
        if self.db.supervisor_record()?.paused_at.is_some() {
            self.db.record_supervisor_tick(pid, Utc::now(), None)?;
            self.publish(&machine.config, number, &TickOutcome::Paused);
            return Ok(TickOutcome::Paused);
        }

        let enabled = load_enabled_projects(self.db, &machine)?;
        let configs: Vec<&Config> = enabled.loaded.iter().collect();
        let stop = || {
            stopping.load(Ordering::SeqCst)
                || self
                    .db
                    .supervisor_record()
                    .map_or(true, |record| record.paused_at.is_some())
        };
        let scheduler = Scheduler {
            db: self.db,
            backends: self.backends,
            budget: TickBudget::from_config(&machine.config),
            stop: &stop,
        };
        let mut report = scheduler
            .tick(&machine.config, &configs, Some(number), &mut memory.turns)
            .await;
        for (project_id, error) in enabled.failed {
            error!(project_id, error = %error, "enabled project not loaded");
            report.projects.entry(project_id).or_default().push(error);
        }

        let finished = Utc::now();
        for (project_id, errors) in &report.projects {
            let error = (!errors.is_empty()).then(|| errors.join("; "));
            self.db
                .record_project_health(project_id, finished, error.as_deref())?;
            let notifications = enabled
                .loaded
                .iter()
                .find(|config| &config.project_id == project_id)
                .map_or(&machine.config.notifications, |config| {
                    &config.notifications
                });
            self.note_project_error(project_id, error, notifications, memory)?;
        }
        let machine_error =
            (!report.machine_errors.is_empty()).then(|| report.machine_errors.join("; "));
        self.db
            .record_supervisor_tick(pid, finished, machine_error.as_deref())?;
        self.note_machine_error(machine_error, &machine.config, memory)?;

        let outcome = TickOutcome::Ran(report);
        self.publish(&machine.config, number, &outcome);
        Ok(outcome)
    }

    fn ensure_same_storage(&self, machine: &MachineConfig) -> Result<()> {
        let record = self.db.supervisor_record()?;
        let state_dir = machine.config.state_dir();
        let db_moved = machine
            .config
            .db_path()
            .is_some_and(|path| path != self.db.path);
        let dir_moved = record
            .state_dir
            .as_deref()
            .is_some_and(|held| held != state_dir);
        if !db_moved && !dir_moved {
            return Ok(());
        }
        Err(StorageMoved {
            old_state_dir: record
                .state_dir
                .map_or_else(String::new, |dir| dir.display().to_string()),
            new_state_dir: state_dir.display().to_string(),
            old_db: self.db.path.display().to_string(),
            new_db: machine
                .config
                .db_path()
                .map_or_else(|| ":memory:".to_string(), |path| path.display().to_string()),
        }
        .into())
    }

    /// Record a project's error once per change, with a notification.
    fn note_project_error(
        &self,
        project_id: &str,
        error: Option<String>,
        notifications: &crate::config::NotificationsConfig,
        memory: &mut SupervisorMemory,
    ) -> Result<()> {
        match error {
            Some(error) if memory.project_errors.get(project_id) != Some(&error) => {
                self.db.add_event(
                    Some(project_id),
                    None,
                    "tick_failed",
                    json!({ "error": error }),
                )?;
                fire_notification(
                    notifications,
                    NotificationKind::TickError,
                    None,
                    None,
                    Some(&format!("{project_id}: {error}")),
                );
                memory.project_errors.insert(project_id.to_string(), error);
            }
            Some(_) => {}
            None => {
                if memory.project_errors.remove(project_id).is_some() {
                    info!(project_id, "project recovered");
                }
            }
        }
        Ok(())
    }

    fn note_machine_error(
        &self,
        error: Option<String>,
        machine: &Config,
        memory: &mut SupervisorMemory,
    ) -> Result<()> {
        if error.is_some() && error != memory.machine_error {
            let message = error.as_deref().unwrap_or_default();
            self.db
                .add_event(None, None, "tick_failed", json!({ "error": message }))?;
            fire_notification(
                &machine.notifications,
                NotificationKind::TickError,
                None,
                None,
                Some(message),
            );
        }
        memory.machine_error = error;
        Ok(())
    }

    /// A tick that could not run at all (global config, database).
    fn machine_failure(&self, pid: u32, err: &anyhow::Error, memory: &mut SupervisorMemory) {
        let message = format!("{err:#}");
        error!(error = %message, "supervisor tick failed");
        if let Err(record_err) = self
            .db
            .record_supervisor_tick(pid, Utc::now(), Some(&message))
        {
            warn!(error = %record_err, "failed to record the supervisor tick failure");
        }
        if memory.machine_error.as_ref() != Some(&message) {
            warn!(error = %message, "supervisor cannot run its projects until this is fixed");
        }
        memory.machine_error = Some(message);
    }

    /// The fleet widget document and the foreground panel. Their failures
    /// are logged only: neither may stop the supervisor.
    fn publish(&self, machine: &Config, number: u64, outcome: &TickOutcome) {
        if let Some(path) = machine.widget_state_path()
            && let Err(err) = widget_state::write_fleet(self.db, &path, Utc::now())
        {
            warn!(error = %format!("{err:#}"), "failed to write widget state file");
        }
        if self.panel
            && let Err(err) = panel::render_supervisor_panel(machine, self.db, number, outcome)
        {
            warn!(error = %format!("{err:#}"), "failed to render the panel");
        }
    }

    fn control_version(&self) -> i64 {
        self.db
            .supervisor_record()
            .map(|record| record.control_version)
            .unwrap_or_default()
    }

    /// Sleep until the next tick, a pause/resume/enable/disable, or shutdown.
    async fn wait(&self, control: i64, stopping: &AtomicBool, wake: &Notify) {
        let deadline = tokio::time::Instant::now() + self.interval;
        loop {
            if stopping.load(Ordering::SeqCst) || self.control_version() != control {
                return;
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return;
            }
            tokio::select! {
                _ = tokio::time::sleep(CONTROL_POLL.min(deadline - now)) => {}
                _ = wake.notified() => return,
            }
        }
    }
}

/// Refresh the supervisor's heartbeat independently of its ticks, which can
/// spend minutes in one provider call.
async fn heartbeat(db: Db, pid: u32) {
    let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);
    loop {
        interval.tick().await;
        match db.supervisor_heartbeat(pid, Utc::now()) {
            Ok(true) => {}
            Ok(false) => warn!(pid, "the supervisor row belongs to another supervisor"),
            Err(err) => warn!(error = %err, "failed to record the supervisor heartbeat"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(heartbeat_age: Option<i64>, paused: bool, stopped: bool) -> SupervisorRecord {
        let now = Utc::now();
        SupervisorRecord {
            heartbeat_at: heartbeat_age.map(|age| now - Duration::seconds(age)),
            paused_at: paused.then_some(now),
            stopped_at: stopped.then_some(now),
            ..SupervisorRecord::default()
        }
    }

    #[test]
    fn state_follows_heartbeat_pause_and_stop() {
        let now = Utc::now();
        assert_eq!(
            SupervisorState::of(&SupervisorRecord::default(), now),
            SupervisorState::Stopped
        );
        assert_eq!(
            SupervisorState::of(&record(Some(5), false, false), now),
            SupervisorState::Running
        );
        assert_eq!(
            SupervisorState::of(&record(Some(5), true, false), now),
            SupervisorState::Paused
        );
        assert_eq!(
            SupervisorState::of(&record(Some(120), false, false), now),
            SupervisorState::Stopped,
            "a stale heartbeat means the supervisor is gone"
        );
        assert_eq!(
            SupervisorState::of(&record(Some(5), false, true), now),
            SupervisorState::Stopped
        );
    }

    #[test]
    fn a_second_lock_on_the_same_state_dir_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let first = SupervisorLock::acquire(tmp.path()).unwrap();
        let err = SupervisorLock::acquire(tmp.path()).unwrap_err().to_string();
        assert!(err.contains("already running"), "{err}");
        assert!(
            err.contains(&format!("pid {}", std::process::id())),
            "{err}"
        );
        drop(first);
        SupervisorLock::acquire(tmp.path()).expect("released with its holder");
    }
}
