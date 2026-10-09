//! Scaffolding shared by the OSS-337 integration tests: a temp-dir project on a
//! file-backed database, a scriptable gated `ReviewBackend`, and event and lease helpers.
// Each test crate uses a different subset of this module.
#![allow(dead_code)]

use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use reviewloop::{
    backend::{BackendError, ReviewBackend, ReviewFetchResult, SubmitReceipt, SubmitRequest},
    config::{Config, PaperConfig},
    db::{Db, JobChange, Lease},
    model::{EventRecord, Job, JobPdf, JobStatus, NewJob},
    submission_input::prepare_input,
};
use rusqlite::{Connection, types::ValueRef};
use serde_json::{Value, json};
use std::{
    collections::{BTreeSet, VecDeque},
    fs,
    future::Future,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration as StdDuration,
};
use tempfile::TempDir;
use tokio::sync::Notify;

pub const PROJECT: &str = "project-oss-337";
pub const PAPER: &str = "main";
pub const EMAIL: &str = "test@example.edu";
/// Mirrors the worker's submit lease length.
pub const SUBMIT_TTL: Duration = Duration::minutes(30);
/// Mirrors the worker's poll lease length.
pub const POLL_TTL: Duration = Duration::minutes(10);
/// Real-time guard for gated tests (never used under the paused clock).
pub const TEST_DEADLINE: StdDuration = StdDuration::from_secs(30);

// ---------------------------------------------------------------------------------------
// Test project
// ---------------------------------------------------------------------------------------

/// One project in a temp dir: its config, a file-backed database and the paper PDF.
pub struct Ctx {
    pub tmp: TempDir,
    pub config: Config,
    pub db: Db,
    pub pdf_path: PathBuf,
}

impl Ctx {
    /// Stanford backend on a closed local port (a stray real-backend call fails fast),
    /// fallback disabled, schedule `[10, 20, 40, 60]` minutes without jitter.
    pub fn new() -> Result<Self> {
        let tmp = tempfile::tempdir()?;
        let state_dir = tmp.path().join("state");
        fs::create_dir_all(&state_dir)?;

        // No `/Type /Page` objects: the review timeout is the configured base, unscaled.
        let pdf_path = tmp.path().join("paper.pdf");
        fs::write(&pdf_path, b"%PDF-1.4\n1 0 obj\n<<>>\nendobj\n%%EOF\n")?;

        let mut config = Config {
            project_id: PROJECT.to_string(),
            ..Config::default()
        };
        config.core.state_dir = state_dir.to_string_lossy().to_string();
        config.core.widget_state_enabled = false;
        config.polling.schedule_minutes = vec![10, 20, 40, 60];
        config.polling.jitter_percent = 0;
        config.trigger.git.enabled = false;
        config.trigger.pdf.enabled = false;
        config.imap = None;
        config.notifications.enabled = false;
        config.providers.stanford.base_url = "http://127.0.0.1:9".to_string();
        config.providers.stanford.email = EMAIL.to_string();
        config.providers.stanford.venue = Some("ICLR".to_string());
        config.providers.stanford.fallback_mode = "disabled".to_string();
        config.papers = vec![PaperConfig {
            id: PAPER.to_string(),
            pdf_path: pdf_path.to_string_lossy().to_string(),
            backend: "stanford".to_string(),
            venue: None,
        }];

        let db = Db::new_file(state_dir.join("reviewloop.db"));
        db.ensure_schema()?;

        Ok(Self {
            tmp,
            config,
            db,
            pdf_path,
        })
    }

    /// Another worker's (process's) handle on the same database file.
    pub fn other_handle(&self) -> Db {
        Db::new_file(self.db.path.clone())
    }

    /// A raw connection on the database file, bypassing the crate's row mapping.
    pub fn conn(&self) -> Result<Connection> {
        let conn = Connection::open(&self.db.path)?;
        conn.busy_timeout(StdDuration::from_secs(5))?;
        Ok(conn)
    }

    pub fn create_queued_job(&self) -> Result<Job> {
        self.db.create_job(&NewJob {
            project_id: PROJECT.to_string(),
            paper_id: PAPER.to_string(),
            backend: "stanford".to_string(),
            pdf: JobPdf::Pinned(prepare_input(&self.config.state_dir(), &self.pdf_path)?),
            status: JobStatus::Queued,
            email: EMAIL.to_string(),
            venue: self.config.providers.stanford.venue.clone(),
            review_options: Default::default(),
            git_tag: None,
            git_commit: None,
            next_poll_at: None,
        })
    }

    /// A PROCESSING job holding `token`, next polled at `next_poll_at`.
    pub fn create_processing_job(&self, token: &str, next_poll_at: DateTime<Utc>) -> Result<Job> {
        let job = self.create_queued_job()?;
        self.db.attach_token_to_job(&job.id, token, next_poll_at)?;
        let job = self.job(&job.id)?;
        assert_eq!(job.status, JobStatus::Processing);
        assert_eq!(job.token.as_deref(), Some(token));
        assert_eq!(job.next_poll_at, Some(next_poll_at));
        assert!(job.started_at.is_some());
        Ok(job)
    }

    pub fn job(&self, job_id: &str) -> Result<Job> {
        load_job(&self.db, job_id)
    }

    /// This job's events, oldest first.
    pub fn events(&self, job_id: &str) -> Result<Vec<EventRecord>> {
        events_for(&self.db, PROJECT, job_id)
    }

    pub fn event_types(&self, job_id: &str) -> Result<Vec<String>> {
        Ok(self
            .events(job_id)?
            .into_iter()
            .map(|event| event.event_type)
            .collect())
    }

    /// Ids of the QUEUED jobs the daemon would submit at `now`.
    pub fn ready_ids(&self, now: DateTime<Utc>) -> Result<BTreeSet<String>> {
        Ok(ids(self.db.list_ready_queued(PROJECT, 1000, now)?))
    }

    /// Ids of the PROCESSING jobs the daemon would poll at `now`.
    pub fn due_ids(&self, now: DateTime<Utc>) -> Result<BTreeSet<String>> {
        Ok(ids(self.db.list_due_processing(PROJECT, 1000, now)?))
    }

    /// Turn the node fallback on with `script`.
    pub fn use_fallback_script(&mut self, script: &Path) {
        self.config.providers.stanford.fallback_mode = "node_playwright".to_string();
        self.config.providers.stanford.fallback_script = script.to_string_lossy().to_string();
    }

    /// Turn the node fallback on with a script made of `body`; returns the script path.
    pub fn arm_fallback_script(&mut self, body: &str) -> Result<PathBuf> {
        let script = self.tmp.path().join("fallback.cjs");
        fs::write(&script, body)?;
        self.use_fallback_script(&script);
        Ok(script)
    }

    /// Arm the node fallback with a script that leaves a marker file behind when it runs
    /// and reports token `tok-fallback`; returns the marker path.
    pub fn arm_marker_fallback(&mut self) -> Result<PathBuf> {
        let marker = self.tmp.path().join("fallback-ran.marker");
        let marker_literal = serde_json::to_string(&marker.to_string_lossy())?;
        self.arm_fallback_script(&format!(
            "const fs = require(\"fs\");\n\
             fs.writeFileSync({marker_literal}, \"fallback ran\\n\");\n\
             console.log(JSON.stringify({{ success: true, token: \"tok-fallback\" }}));\n"
        ))?;
        Ok(marker)
    }
}

fn ids(jobs: Vec<Job>) -> BTreeSet<String> {
    jobs.into_iter().map(|job| job.id).collect()
}

pub fn load_job(db: &Db, job_id: &str) -> Result<Job> {
    db.get_job(job_id)?
        .with_context(|| format!("job {job_id} not found"))
}

/// The job's events, oldest first; the timeline query returns every job of the paper.
pub fn events_for(db: &Db, project_id: &str, job_id: &str) -> Result<Vec<EventRecord>> {
    let mut events: Vec<EventRecord> = db
        .list_timeline_events(project_id, PAPER)?
        .into_iter()
        .filter(|event| event.job_id.as_deref() == Some(job_id))
        .collect();
    events.sort_by_key(|event| event.id);
    Ok(events)
}

pub fn node_available() -> bool {
    Command::new("node")
        .arg("--version")
        .output()
        .is_ok_and(|out| out.status.success())
}

// ---------------------------------------------------------------------------------------
// Events, leases and job changes
// ---------------------------------------------------------------------------------------

pub fn event_types(events: &[EventRecord]) -> Vec<&str> {
    events
        .iter()
        .map(|event| event.event_type.as_str())
        .collect()
}

pub fn count_events(events: &[EventRecord], event_type: &str) -> usize {
    events
        .iter()
        .filter(|event| event.event_type == event_type)
        .count()
}

/// The payload of the only event of `event_type`; fails when there is not exactly one.
pub fn only_event<'a>(events: &'a [EventRecord], event_type: &str) -> &'a Value {
    let matching: Vec<&EventRecord> = events
        .iter()
        .filter(|event| event.event_type == event_type)
        .collect();
    assert_eq!(
        matching.len(),
        1,
        "expected exactly one `{event_type}` event, got timeline {:?}",
        event_types(events)
    );
    &matching[0].payload
}

/// Channels of the `submit_dispatched` events, in order.
pub fn dispatch_channels(events: &[EventRecord]) -> Vec<&str> {
    events
        .iter()
        .filter(|event| event.event_type == "submit_dispatched")
        .map(|event| event.payload["channel"].as_str().unwrap_or("?"))
        .collect()
}

pub fn assert_no_lease(job: &Job) {
    assert_eq!(job.lease_owner, None, "lease owner must be cleared");
    assert_eq!(job.lease_expires_at, None, "lease expiry must be cleared");
}

pub fn assert_held_by(job: &Job, lease: &Lease) {
    assert_eq!(job.lease_owner.as_deref(), Some(lease.owner.as_str()));
    assert_eq!(job.lease_expires_at, Some(lease.expires_at));
}

pub fn diagnostic(job: &Job) -> Result<&str> {
    job.last_error
        .as_deref()
        .context("job must carry a diagnostic in last_error")
}

/// A poll that completed on its first attempt.
pub fn completed_change() -> JobChange {
    JobChange {
        status: JobStatus::Completed,
        attempt: Some(1),
        next_poll_at: Some(None),
        last_error: Some(None),
        submit_stage: None,
        fallback_used: None,
    }
}

// ---------------------------------------------------------------------------------------
// Secret containment
// ---------------------------------------------------------------------------------------

/// Tables [`assert_secret_absent`] reads in full.
const SECRET_SCANNED_TABLES: [&str; 4] = ["jobs", "events", "enqueue_requests", "reviews"];

/// Fail when `secret` appears anywhere the project keeps or prints state: any column of
/// the job, event, request-key and review tables, the bytes of any file under the state
/// dir (the database and its WAL included, so even overwritten rows count), or the
/// config's `Debug` and serialized provider settings.
pub fn assert_secret_absent(ctx: &Ctx, secret: &str) -> Result<()> {
    assert!(!secret.is_empty(), "an empty secret would match everything");
    let conn = ctx.conn()?;
    for table in SECRET_SCANNED_TABLES {
        let rows = table_text(&conn, table)?;
        if table == "jobs" {
            assert!(!rows.is_empty(), "nothing to scan: the jobs table is empty");
        }
        for (index, row) in rows.iter().enumerate() {
            assert!(
                !row.iter().any(|cell| cell.contains(secret)),
                "secret stored in {table} row {index}: {row:?}"
            );
        }
    }

    let files = files_under(&ctx.config.state_dir())?;
    assert!(!files.is_empty(), "nothing to scan: the state dir is empty");
    for file in files {
        let bytes = fs::read(&file).with_context(|| format!("reading {}", file.display()))?;
        assert!(
            !bytes
                .windows(secret.len())
                .any(|window| window == secret.as_bytes()),
            "secret written to {}",
            file.display()
        );
    }

    assert!(
        !format!("{:?}", ctx.config).contains(secret),
        "secret in the config's Debug output"
    );
    assert!(
        !serde_json::to_string(&ctx.config.providers)?.contains(secret),
        "secret in the serialized provider settings"
    );
    Ok(())
}

/// Every row of `table`, each column rendered as text (`NULL` as empty).
fn table_text(conn: &Connection, table: &str) -> Result<Vec<Vec<String>>> {
    let mut stmt = conn.prepare(&format!("SELECT * FROM {table}"))?;
    let columns = stmt.column_count();
    let rows = stmt.query_map([], |row| {
        (0..columns)
            .map(|index| {
                Ok(match row.get_ref(index)? {
                    ValueRef::Null => String::new(),
                    ValueRef::Integer(value) => value.to_string(),
                    ValueRef::Real(value) => value.to_string(),
                    ValueRef::Text(bytes) | ValueRef::Blob(bytes) => {
                        String::from_utf8_lossy(bytes).into_owned()
                    }
                })
            })
            .collect::<rusqlite::Result<Vec<String>>>()
    })?;
    rows.collect::<rusqlite::Result<_>>()
        .with_context(|| format!("reading table {table}"))
}

/// Every regular file below `root`, recursively.
fn files_under(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in fs::read_dir(&dir).with_context(|| format!("listing {}", dir.display()))? {
            let path = entry?.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                files.push(path);
            }
        }
    }
    Ok(files)
}

/// Fail instead of hanging when a gate is never opened.
pub async fn with_deadline<T>(fut: impl Future<Output = T>) -> Result<T> {
    tokio::time::timeout(TEST_DEADLINE, fut)
        .await
        .context("worker test deadlocked")
}

// ---------------------------------------------------------------------------------------
// Mock backend
// ---------------------------------------------------------------------------------------

pub type SubmitResult = std::result::Result<SubmitReceipt, BackendError>;
pub type FetchResult = std::result::Result<ReviewFetchResult, BackendError>;

/// How one mock backend call answers.
pub enum Answer<T> {
    /// Return at once.
    Now(T),
    /// Signal `entered`, wait for `release`, then return: the request has reached the
    /// provider and the test acts while the worker awaits the answer. Never releasing it
    /// models a hung provider.
    OnRelease(T),
    /// Never return.
    Never,
}

type Script<T> = Box<dyn Fn() -> Answer<T> + Send + Sync>;

/// A `ReviewBackend` whose every call answers from a script; unscripted calls fail.
pub struct MockBackend {
    submit: Script<SubmitResult>,
    fetch: Script<FetchResult>,
    submits: AtomicUsize,
    fetches: AtomicUsize,
    fetched_tokens: Mutex<Vec<String>>,
    /// Fires once an [`Answer::OnRelease`] call has entered the backend. `notify_one`
    /// keeps a permit when nobody waits yet, so neither signal is lost.
    pub entered: Notify,
    pub release: Notify,
}

impl Default for MockBackend {
    fn default() -> Self {
        Self {
            submit: Box::new(|| {
                Answer::Now(Err(BackendError::Network(
                    "mock: submit was not expected".to_string(),
                )))
            }),
            fetch: Box::new(|| {
                Answer::Now(Err(BackendError::Network(
                    "mock: fetch was not expected".to_string(),
                )))
            }),
            submits: AtomicUsize::new(0),
            fetches: AtomicUsize::new(0),
            fetched_tokens: Mutex::new(Vec::new()),
            entered: Notify::new(),
            release: Notify::new(),
        }
    }
}

impl MockBackend {
    /// Every submit answers `script()`.
    pub fn on_submit(
        mut self,
        script: impl Fn() -> Answer<SubmitResult> + Send + Sync + 'static,
    ) -> Self {
        self.submit = Box::new(script);
        self
    }

    /// Every fetch answers `script()`.
    pub fn on_fetch(
        mut self,
        script: impl Fn() -> Answer<FetchResult> + Send + Sync + 'static,
    ) -> Self {
        self.fetch = Box::new(script);
        self
    }

    pub fn submit_count(&self) -> usize {
        self.submits.load(Ordering::SeqCst)
    }

    pub fn fetch_count(&self) -> usize {
        self.fetches.load(Ordering::SeqCst)
    }

    pub fn fetched_tokens(&self) -> Vec<String> {
        self.fetched_tokens
            .lock()
            .expect("fetched_tokens poisoned")
            .clone()
    }

    async fn answer<T>(&self, answer: Answer<T>) -> T {
        match answer {
            Answer::Now(value) => value,
            Answer::OnRelease(value) => {
                self.entered.notify_one();
                self.release.notified().await;
                value
            }
            Answer::Never => std::future::pending().await,
        }
    }
}

#[async_trait]
impl ReviewBackend for MockBackend {
    fn name(&self) -> &'static str {
        "mock"
    }

    async fn submit(&self, _req: SubmitRequest) -> SubmitResult {
        self.submits.fetch_add(1, Ordering::SeqCst);
        self.answer((self.submit)()).await
    }

    async fn fetch_review(&self, token: &str) -> FetchResult {
        self.fetches.fetch_add(1, Ordering::SeqCst);
        self.fetched_tokens
            .lock()
            .expect("fetched_tokens poisoned")
            .push(token.to_string());
        self.answer((self.fetch)()).await
    }
}

/// A script answering with `answers` in order, one per call; once they run out every
/// call fails.
pub fn in_order<T: Send + 'static>(
    answers: Vec<Answer<std::result::Result<T, BackendError>>>,
) -> impl Fn() -> Answer<std::result::Result<T, BackendError>> + Send + Sync {
    let queue = Mutex::new(VecDeque::from(answers));
    move || {
        queue
            .lock()
            .expect("mock answer queue poisoned")
            .pop_front()
            .unwrap_or_else(|| {
                Answer::Now(Err(BackendError::Schema(
                    "mock reply queue is empty".to_string(),
                )))
            })
    }
}

pub fn receipt(token: &str) -> SubmitReceipt {
    SubmitReceipt {
        token: token.to_string(),
    }
}

pub fn review_json() -> Value {
    json!({
        "title": "Sample Paper",
        "venue": "ICLR",
        "sections": { "summary": "Solid work" }
    })
}

pub fn ready_review() -> ReviewFetchResult {
    ReviewFetchResult::Ready {
        raw_json: review_json(),
    }
}
