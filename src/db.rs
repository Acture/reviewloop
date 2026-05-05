use crate::{
    config::Config,
    model::{EventRecord, Job, JobStatus, NewJob, StatusView},
    util::{parse_rfc3339, to_rfc3339},
};
use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params, params_from_iter};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Duration,
};
use uuid::Uuid;

#[derive(Debug, Default, Clone, Copy)]
pub struct PruneReport {
    pub email_tokens: usize,
    pub seen_tags: usize,
    pub events: usize,
    pub reviews: usize,
    pub jobs: usize,
}

impl PruneReport {
    pub fn total_deleted(self) -> usize {
        self.email_tokens + self.seen_tags + self.events + self.reviews + self.jobs
    }
}

#[derive(Debug, Default, Clone)]
pub struct PurgePaperReport {
    pub job_ids: Vec<String>,
    pub jobs: usize,
    pub events: usize,
    pub reviews: usize,
}

pub struct Db {
    pub path: PathBuf,
    dsn: String,
    open_flags: OpenFlags,
    keepalive: Option<Connection>,
}

impl std::fmt::Debug for Db {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Db")
            .field("path", &self.path)
            .field("dsn", &self.dsn)
            .field("open_flags", &self.open_flags.bits())
            .field("is_in_memory", &self.keepalive.is_some())
            .finish()
    }
}

impl Db {
    pub fn new(state_dir: &Path) -> Self {
        Self::new_file(state_dir.join("reviewloop.db"))
    }

    pub fn new_file(path: PathBuf) -> Self {
        // C4: set 0o600 on the DB file at creation time (Unix only).
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if !path.exists() {
                // Touch the file so we can set permissions before SQLite opens it.
                if let Ok(f) = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&path)
                {
                    drop(f);
                    let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
                }
            }
        }
        Self {
            dsn: path.to_string_lossy().to_string(),
            path,
            open_flags: OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
            keepalive: None,
        }
    }

    pub fn new_in_memory(name: &str) -> Result<Self> {
        let uri = format!("file:{name}?mode=memory&cache=shared");
        let open_flags = OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_URI;
        let keepalive = Connection::open_with_flags(&uri, open_flags)
            .with_context(|| format!("failed to open sqlite in-memory database: {uri}"))?;
        keepalive.busy_timeout(Duration::from_secs(5))?;

        Ok(Self {
            path: PathBuf::from(":memory:"),
            dsn: uri,
            open_flags,
            keepalive: Some(keepalive),
        })
    }

    pub fn from_config(config: &Config) -> Result<Self> {
        if config.db_in_memory() {
            let memory_name = format!("reviewloop-{}", Uuid::new_v4());
            return Self::new_in_memory(&memory_name);
        }

        let path = config
            .db_path()
            .ok_or_else(|| anyhow!("core.db_path must be set when db is not in-memory"))?;
        Ok(Self::new_file(path))
    }

    fn connect(&self) -> Result<Connection> {
        let conn = Connection::open_with_flags(&self.dsn, self.open_flags).map_err(|e| {
            let is_permission = e.to_string().to_lowercase().contains("permission denied")
                || e.to_string().to_lowercase().contains("unable to open");
            let ctx = if is_permission {
                format!(
                    "failed to open sqlite database: {}; ensure the file is owned by your \
                         user — if you previously ran reviewloop with sudo, run \
                         `sudo chown $(whoami) {}` or remove the file and re-init",
                    self.dsn, self.dsn
                )
            } else {
                format!("failed to open sqlite database: {}", self.dsn)
            };
            anyhow::Error::from(e).context(ctx)
        })?;
        // 30-second busy timeout so concurrent writes from emit_failover_event
        // (opening a fresh connection while update_job_state holds a write
        // transaction) retry rather than fail immediately. WAL mode (set in
        // init_schema) further reduces contention, but having a generous
        // timeout is a belt-and-suspenders safeguard.
        conn.busy_timeout(Duration::from_secs(30))?;
        Ok(conn)
    }

    pub fn init_schema(&self) -> Result<()> {
        let conn = self.connect()?;
        // Enable WAL journal mode for file-based databases.  WAL allows
        // concurrent readers + one writer without blocking each other, so a
        // write transaction in one connection (e.g. update_job_state) does
        // not starve another connection's write (e.g. emit_failover_event).
        // For in-memory databases this pragma is silently ignored (mode stays
        // "memory"), which is fine since in-memory DBs are single-process
        // and don't have the concurrent-connection issue.
        let _ = conn.execute_batch("PRAGMA journal_mode = WAL;");

        // Step 1: create tables. CREATE TABLE IF NOT EXISTS leaves an
        // existing table's schema untouched, so on upgrade these no-op
        // and we rely on ensure_column_exists below to backfill any
        // columns that didn't exist in the older schema.
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL DEFAULT '',
                paper_id TEXT NOT NULL,
                backend TEXT NOT NULL,
                pdf_path TEXT NOT NULL,
                pdf_hash TEXT NOT NULL,
                status TEXT NOT NULL,
                token TEXT,
                email TEXT NOT NULL,
                venue TEXT,
                git_tag TEXT,
                git_commit TEXT,
                version_no INTEGER NOT NULL DEFAULT 1,
                round_no INTEGER NOT NULL DEFAULT 1,
                version_source TEXT NOT NULL DEFAULT 'pdf_hash',
                version_key TEXT NOT NULL DEFAULT '',
                attempt INTEGER NOT NULL DEFAULT 0,
                started_at TEXT,
                next_poll_at TEXT,
                last_error TEXT,
                fallback_used INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS reviews (
                job_id TEXT PRIMARY KEY,
                token TEXT NOT NULL,
                raw_json TEXT NOT NULL,
                summary_md TEXT NOT NULL,
                completed_at TEXT NOT NULL,
                FOREIGN KEY(job_id) REFERENCES jobs(id)
            );

            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id TEXT NOT NULL DEFAULT '',
                job_id TEXT,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS seen_tags (
                tag_name TEXT PRIMARY KEY,
                target_commit TEXT NOT NULL,
                seen_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS email_tokens (
                token TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                matched_at TEXT NOT NULL,
                raw_ref TEXT
            );
            "#,
        )?;

        // Step 2: backfill columns that were added in later versions. MUST
        // run before CREATE INDEX below, since some indexes reference
        // columns (project_id, version_key) that an older schema lacks --
        // creating those indexes against a pre-migration table would fail
        // with "no such column".
        ensure_column_exists(&conn, "jobs", "project_id", "TEXT NOT NULL DEFAULT ''")?;
        ensure_column_exists(&conn, "jobs", "started_at", "TEXT")?;
        ensure_column_exists(&conn, "jobs", "version_no", "INTEGER NOT NULL DEFAULT 1")?;
        ensure_column_exists(&conn, "jobs", "round_no", "INTEGER NOT NULL DEFAULT 1")?;
        ensure_column_exists(
            &conn,
            "jobs",
            "version_source",
            "TEXT NOT NULL DEFAULT 'pdf_hash'",
        )?;
        ensure_column_exists(&conn, "jobs", "version_key", "TEXT NOT NULL DEFAULT ''")?;
        ensure_column_exists(&conn, "events", "project_id", "TEXT NOT NULL DEFAULT ''")?;

        // Step 3: indexes (now that all referenced columns exist).
        conn.execute_batch(
            r#"
            CREATE INDEX IF NOT EXISTS idx_jobs_project_status_next_poll ON jobs(project_id, status, next_poll_at);
            CREATE INDEX IF NOT EXISTS idx_jobs_project_backend_hash ON jobs(project_id, backend, pdf_hash);
            CREATE INDEX IF NOT EXISTS idx_jobs_project_paper_backend ON jobs(project_id, paper_id, backend);
            CREATE INDEX IF NOT EXISTS idx_jobs_project_dedupe ON jobs(project_id, paper_id, backend, pdf_hash, version_key, status);
            CREATE INDEX IF NOT EXISTS idx_events_project_created_at ON events(project_id, created_at);
            "#,
        )?;

        conn.execute(
            "UPDATE jobs SET version_no = 1 WHERE version_no IS NULL OR version_no = 0",
            [],
        )?;
        conn.execute(
            "UPDATE jobs SET round_no = 1 WHERE round_no IS NULL OR round_no = 0",
            [],
        )?;
        conn.execute(
            r#"
            UPDATE jobs
            SET version_source = CASE
                    WHEN COALESCE(TRIM(git_commit), '') <> '' THEN 'git_commit'
                    ELSE 'pdf_hash'
                END
            WHERE COALESCE(TRIM(version_source), '') = ''
            "#,
            [],
        )?;
        conn.execute(
            r#"
            UPDATE jobs
            SET version_key = CASE
                    WHEN COALESCE(TRIM(git_commit), '') <> '' THEN git_commit
                    ELSE pdf_hash
                END
            WHERE COALESCE(TRIM(version_key), '') = ''
            "#,
            [],
        )?;
        conn.execute(
            r#"
            UPDATE events
            SET project_id = COALESCE((SELECT jobs.project_id FROM jobs WHERE jobs.id = events.job_id), '')
            WHERE COALESCE(project_id, '') = ''
            "#,
            [],
        )?;
        Ok(())
    }

    pub fn assign_unscoped_rows_to_project(&self, project_id: &str) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            "UPDATE jobs SET project_id = ?1 WHERE COALESCE(project_id, '') = ''",
            params![project_id],
        )?;
        conn.execute(
            "UPDATE events SET project_id = ?1 WHERE COALESCE(project_id, '') = ''",
            params![project_id],
        )?;
        Ok(())
    }

    pub fn create_job(&self, new_job: &NewJob) -> Result<Job> {
        let now = Utc::now();
        let id = Uuid::new_v4().to_string();
        let conn = self.connect()?;
        let (version_no, round_no, version_source, version_key) =
            determine_versioning(&conn, new_job)?;

        conn.execute(
            r#"
            INSERT INTO jobs (
                id, project_id, paper_id, backend, pdf_path, pdf_hash, status, token, email, venue,
                git_tag, git_commit, version_no, round_no, version_source, version_key,
                attempt, started_at, next_poll_at, last_error, fallback_used, created_at, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, 0, NULL, ?16, NULL, 0, ?17, ?17)
            "#,
            params![
                id,
                new_job.project_id,
                new_job.paper_id,
                new_job.backend,
                new_job.pdf_path,
                new_job.pdf_hash,
                new_job.status.as_str(),
                new_job.email,
                new_job.venue,
                new_job.git_tag,
                new_job.git_commit,
                version_no as i64,
                round_no as i64,
                version_source,
                version_key,
                new_job.next_poll_at.map(to_rfc3339),
                to_rfc3339(now),
            ],
        )?;

        self.get_job(&id)?
            .ok_or_else(|| anyhow!("failed to load inserted job: {id}"))
    }

    pub fn get_job(&self, job_id: &str) -> Result<Option<Job>> {
        let conn = self.connect()?;
        conn.query_row(
            "SELECT * FROM jobs WHERE id = ?1",
            params![job_id],
            map_job_row,
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn get_project_job(&self, project_id: &str, job_id: &str) -> Result<Option<Job>> {
        let conn = self.connect()?;
        conn.query_row(
            "SELECT * FROM jobs WHERE project_id = ?1 AND id = ?2",
            params![project_id, job_id],
            map_job_row,
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn list_status_views(
        &self,
        project_id: &str,
        paper_id: Option<&str>,
    ) -> Result<Vec<StatusView>> {
        let conn = self.connect()?;
        let mut out = Vec::new();

        let sql = r#"
            SELECT
                j.id,
                j.project_id,
                j.paper_id,
                j.backend,
                j.status,
                j.token,
                j.attempt,
                j.created_at,
                j.started_at,
                j.next_poll_at,
                j.updated_at,
                j.last_error,
                j.pdf_hash,
                j.git_tag,
                j.git_commit,
                j.version_no,
                j.round_no,
                j.version_source,
                j.version_key,
                r.raw_json,
                r.summary_md,
                r.completed_at
            FROM jobs j
            LEFT JOIN reviews r ON r.job_id = j.id
            WHERE j.project_id = ?1
              AND (?2 IS NULL OR j.paper_id = ?2)
            ORDER BY j.created_at DESC
            LIMIT 200
        "#;
        let mut stmt = conn.prepare(sql)?;
        let rows = stmt.query_map(params![project_id, paper_id], map_status_row)?;
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    pub fn list_timeline_events(
        &self,
        project_id: &str,
        paper_id: &str,
    ) -> Result<Vec<EventRecord>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(
            r#"
            SELECT id, project_id, job_id, event_type, payload_json, created_at
            FROM events
            WHERE project_id = ?1
              AND (
                job_id IN (SELECT id FROM jobs WHERE project_id = ?1 AND paper_id = ?2)
                OR JSON_EXTRACT(payload_json, '$.paper_id') = ?2
              )
            ORDER BY created_at ASC, id ASC
            "#,
        )?;
        let rows = stmt.query_map(params![project_id, paper_id], map_event_row)?;
        collect_rows(rows)
    }

    pub fn find_duplicate_covering_job(
        &self,
        project_id: &str,
        paper_id: &str,
        backend: &str,
        pdf_hash: &str,
        version_key: &str,
    ) -> Result<Option<Job>> {
        let conn = self.connect()?;
        conn.query_row(
            r#"
            SELECT *
            FROM jobs
            WHERE project_id = ?1
              AND paper_id = ?2
              AND backend = ?3
              AND pdf_hash = ?4
              AND version_key = ?5
              AND status IN (?6, ?7, ?8, ?9, ?10)
            ORDER BY created_at DESC
            LIMIT 1
            "#,
            params![
                project_id,
                paper_id,
                backend,
                pdf_hash,
                version_key,
                JobStatus::PendingApproval.as_str(),
                JobStatus::Queued.as_str(),
                JobStatus::Submitted.as_str(),
                JobStatus::Processing.as_str(),
                JobStatus::Completed.as_str(),
            ],
            map_job_row,
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn list_active_jobs_for_paper(&self, project_id: &str, paper_id: &str) -> Result<Vec<Job>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(
            r#"
            SELECT *
            FROM jobs
            WHERE project_id = ?1
              AND paper_id = ?2
              AND status IN (?3, ?4, ?5)
            ORDER BY created_at DESC
            "#,
        )?;
        let rows = stmt.query_map(
            params![
                project_id,
                paper_id,
                JobStatus::Queued.as_str(),
                JobStatus::Submitted.as_str(),
                JobStatus::Processing.as_str(),
            ],
            map_job_row,
        )?;
        collect_rows(rows)
    }

    pub fn latest_hash_for_paper(
        &self,
        project_id: &str,
        paper_id: &str,
        backend: &str,
    ) -> Result<Option<String>> {
        let conn = self.connect()?;
        conn.query_row(
            r#"
            SELECT pdf_hash
            FROM jobs
            WHERE project_id = ?1 AND paper_id = ?2 AND backend = ?3
            ORDER BY created_at DESC
            LIMIT 1
            "#,
            params![project_id, paper_id, backend],
            |row| row.get(0),
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn list_ready_queued(
        &self,
        project_id: &str,
        limit: usize,
        now: DateTime<Utc>,
    ) -> Result<Vec<Job>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(
            r#"
            SELECT *
            FROM jobs
            WHERE project_id = ?1
              AND status = ?2
              AND (next_poll_at IS NULL OR next_poll_at <= ?3)
            ORDER BY created_at ASC
            LIMIT ?4
            "#,
        )?;
        let rows = stmt.query_map(
            params![
                project_id,
                JobStatus::Queued.as_str(),
                to_rfc3339(now),
                limit as i64
            ],
            map_job_row,
        )?;
        collect_rows(rows)
    }

    pub fn list_due_processing(
        &self,
        project_id: &str,
        limit: usize,
        now: DateTime<Utc>,
    ) -> Result<Vec<Job>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(
            r#"
            SELECT *
            FROM jobs
            WHERE project_id = ?1
              AND status = ?2
              AND token IS NOT NULL
              AND (next_poll_at IS NULL OR next_poll_at <= ?3)
            ORDER BY COALESCE(next_poll_at, created_at) ASC
            LIMIT ?4
            "#,
        )?;
        let rows = stmt.query_map(
            params![
                project_id,
                JobStatus::Processing.as_str(),
                to_rfc3339(now),
                limit as i64
            ],
            map_job_row,
        )?;
        collect_rows(rows)
    }

    /// Update job state, enforcing `JobStatus::can_transition` as a guard.
    /// Returns an error on invalid transitions. Use [`update_job_state_unchecked`]
    /// for deliberate user overrides (retry --force, complete, cancel) that
    /// legitimately move out of terminal states.
    pub fn update_job_state(
        &self,
        job_id: &str,
        status: JobStatus,
        attempt: Option<u32>,
        next_poll_at: Option<Option<DateTime<Utc>>>,
        last_error: Option<Option<String>>,
    ) -> Result<()> {
        // Fetch current state to validate the transition before mutating.
        let current = {
            let conn = self.connect()?;
            conn.query_row(
                "SELECT * FROM jobs WHERE id = ?1",
                params![job_id],
                map_job_row,
            )
            .optional()?
            .ok_or_else(|| anyhow!("job not found: {job_id}"))?
        };
        if !current.status.can_transition(status) {
            anyhow::bail!(
                "invalid status transition for job {}: {} -> {}",
                job_id,
                current.status.as_str(),
                status.as_str()
            );
        }
        self.update_job_state_unchecked(job_id, status, attempt, next_poll_at, last_error)
    }

    /// Update job state without enforcing the state-machine guard.
    /// Use at CLI override sites (cmd_retry --force, cmd_complete, cmd_cancel)
    /// that deliberately move jobs out of terminal or otherwise-restricted states.
    pub fn update_job_state_unchecked(
        &self,
        job_id: &str,
        status: JobStatus,
        attempt: Option<u32>,
        next_poll_at: Option<Option<DateTime<Utc>>>,
        last_error: Option<Option<String>>,
    ) -> Result<()> {
        let mut conn = self.connect()?;
        let tx = conn.transaction()?;

        let current = tx
            .query_row(
                "SELECT * FROM jobs WHERE id = ?1",
                params![job_id],
                map_job_row,
            )
            .optional()?
            .ok_or_else(|| anyhow!("job not found: {job_id}"))?;

        let attempt_val = attempt.unwrap_or(current.attempt);
        let next_poll_val = next_poll_at.unwrap_or(current.next_poll_at).map(to_rfc3339);
        let last_error_val = last_error.unwrap_or(current.last_error);

        tx.execute(
            r#"
            UPDATE jobs
            SET status = ?2,
                attempt = ?3,
                next_poll_at = ?4,
                last_error = ?5,
                updated_at = ?6
            WHERE id = ?1
            "#,
            params![
                job_id,
                status.as_str(),
                attempt_val as i64,
                next_poll_val,
                last_error_val,
                to_rfc3339(Utc::now()),
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn mark_submitted_with_token(
        &self,
        job_id: &str,
        token: &str,
        next_poll_at: DateTime<Utc>,
    ) -> Result<()> {
        let mut conn = self.connect()?;
        let tx = conn.transaction()?;

        let current = tx
            .query_row(
                "SELECT * FROM jobs WHERE id = ?1",
                params![job_id],
                map_job_row,
            )
            .optional()?
            .ok_or_else(|| anyhow!("job not found: {job_id}"))?;

        if !current.status.can_transition(JobStatus::Processing) {
            anyhow::bail!(
                "invalid status transition for job {}: {} -> {}",
                job_id,
                current.status.as_str(),
                JobStatus::Processing.as_str()
            );
        }

        let now = to_rfc3339(Utc::now());
        tx.execute(
            r#"
            UPDATE jobs
            SET status = ?2,
                token = ?3,
                started_at = COALESCE(started_at, ?5),
                next_poll_at = ?4,
                last_error = NULL,
                attempt = 0,
                updated_at = ?6
            WHERE id = ?1
            "#,
            params![
                job_id,
                JobStatus::Processing.as_str(),
                token,
                to_rfc3339(next_poll_at),
                now,
                now,
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn mark_fallback_used(&self, job_id: &str) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            "UPDATE jobs SET fallback_used = 1, updated_at = ?2 WHERE id = ?1",
            params![job_id, to_rfc3339(Utc::now())],
        )?;
        Ok(())
    }

    pub fn upsert_review(
        &self,
        job_id: &str,
        token: &str,
        raw_json: &str,
        summary_md: &str,
    ) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            r#"
            INSERT INTO reviews(job_id, token, raw_json, summary_md, completed_at)
            VALUES(?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(job_id) DO UPDATE SET
                token = excluded.token,
                raw_json = excluded.raw_json,
                summary_md = excluded.summary_md,
                completed_at = excluded.completed_at
            "#,
            params![job_id, token, raw_json, summary_md, to_rfc3339(Utc::now())],
        )?;
        Ok(())
    }

    pub fn add_event(
        &self,
        project_id: Option<&str>,
        job_id: Option<&str>,
        event_type: &str,
        payload: Value,
    ) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            r#"
            INSERT INTO events(project_id, job_id, event_type, payload_json, created_at)
            VALUES (
                COALESCE(?1, COALESCE((SELECT jobs.project_id FROM jobs WHERE jobs.id = ?2), '')),
                ?2,
                ?3,
                ?4,
                ?5
            )
            "#,
            params![
                project_id,
                job_id,
                event_type,
                payload.to_string(),
                to_rfc3339(Utc::now()),
            ],
        )?;
        Ok(())
    }

    pub fn is_tag_seen(&self, tag_name: &str) -> Result<bool> {
        let conn = self.connect()?;
        let seen: Option<i64> = conn
            .query_row(
                "SELECT 1 FROM seen_tags WHERE tag_name = ?1 LIMIT 1",
                params![tag_name],
                |row| row.get(0),
            )
            .optional()?;
        Ok(seen.is_some())
    }

    pub fn mark_tag_seen(&self, tag_name: &str, target_commit: &str) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            r#"
            INSERT INTO seen_tags(tag_name, target_commit, seen_at)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(tag_name) DO UPDATE SET
                target_commit = excluded.target_commit,
                seen_at = excluded.seen_at
            "#,
            params![tag_name, target_commit, to_rfc3339(Utc::now())],
        )?;
        Ok(())
    }

    pub fn find_latest_open_job_for_paper(
        &self,
        project_id: &str,
        paper_id: &str,
    ) -> Result<Option<Job>> {
        let conn = self.connect()?;
        conn.query_row(
            r#"
            SELECT *
            FROM jobs
            WHERE project_id = ?1
              AND paper_id = ?2
              AND status NOT IN (?3, ?4, ?5, ?6)
            ORDER BY created_at DESC
            LIMIT 1
            "#,
            params![
                project_id,
                paper_id,
                JobStatus::Completed.as_str(),
                JobStatus::Failed.as_str(),
                JobStatus::FailedNeedsManual.as_str(),
                JobStatus::Timeout.as_str()
            ],
            map_job_row,
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn find_latest_open_job_without_token(
        &self,
        project_id: &str,
        backend: &str,
    ) -> Result<Option<Job>> {
        let conn = self.connect()?;
        conn.query_row(
            r#"
            SELECT *
            FROM jobs
            WHERE project_id = ?1
              AND backend = ?2
              AND token IS NULL
              AND status IN (?3, ?4, ?5, ?6)
            ORDER BY created_at DESC
            LIMIT 1
            "#,
            params![
                project_id,
                backend,
                JobStatus::PendingApproval.as_str(),
                JobStatus::Queued.as_str(),
                JobStatus::Submitted.as_str(),
                JobStatus::Processing.as_str()
            ],
            map_job_row,
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn find_job_by_token(&self, project_id: &str, token: &str) -> Result<Option<Job>> {
        let conn = self.connect()?;
        conn.query_row(
            "SELECT * FROM jobs WHERE project_id = ?1 AND token = ?2 LIMIT 1",
            params![project_id, token],
            map_job_row,
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn attach_token_to_job(
        &self,
        job_id: &str,
        token: &str,
        next_poll_at: DateTime<Utc>,
    ) -> Result<()> {
        self.mark_submitted_with_token(job_id, token, next_poll_at)
    }

    pub fn list_processing_jobs(&self, project_id: &str) -> Result<Vec<Job>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare("SELECT * FROM jobs WHERE project_id = ?1 AND status = ?2")?;
        let rows = stmt.query_map(
            params![project_id, JobStatus::Processing.as_str()],
            map_job_row,
        )?;
        collect_rows(rows)
    }

    pub fn record_email_token(
        &self,
        token: &str,
        source: &str,
        raw_ref: Option<&str>,
    ) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            r#"
            INSERT INTO email_tokens(token, source, matched_at, raw_ref)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(token) DO UPDATE SET
                source = excluded.source,
                matched_at = excluded.matched_at,
                raw_ref = excluded.raw_ref
            "#,
            params![token, source, to_rfc3339(Utc::now()), raw_ref],
        )?;
        Ok(())
    }

    pub fn purge_paper_history(
        &self,
        project_id: &str,
        paper_id: &str,
    ) -> Result<PurgePaperReport> {
        let mut conn = self.connect()?;
        let tx = conn.transaction()?;

        let mut stmt = tx.prepare("SELECT id FROM jobs WHERE project_id = ?1 AND paper_id = ?2")?;
        let iter = stmt.query_map(params![project_id, paper_id], |row| row.get::<_, String>(0))?;
        let mut job_ids = Vec::new();
        for id in iter {
            job_ids.push(id?);
        }
        drop(stmt);

        let reviews = tx.execute(
            "DELETE FROM reviews WHERE job_id IN (SELECT id FROM jobs WHERE project_id = ?1 AND paper_id = ?2)",
            params![project_id, paper_id],
        )?;
        let events = tx.execute(
            "DELETE FROM events WHERE project_id = ?1 AND (job_id IN (SELECT id FROM jobs WHERE project_id = ?1 AND paper_id = ?2) OR json_extract(payload_json, '$.paper_id') = ?2)",
            params![project_id, paper_id],
        )?;
        let jobs = tx.execute(
            "DELETE FROM jobs WHERE project_id = ?1 AND paper_id = ?2",
            params![project_id, paper_id],
        )?;

        tx.commit()?;
        Ok(PurgePaperReport {
            job_ids,
            jobs,
            events,
            reviews,
        })
    }

    pub fn prune_retention(
        &self,
        retention: &crate::config::RetentionConfig,
        now: DateTime<Utc>,
    ) -> Result<PruneReport> {
        if !retention.enabled {
            return Ok(PruneReport::default());
        }

        let mut report = PruneReport::default();
        let mut conn = self.connect()?;
        let tx = conn.transaction()?;

        if retention.email_tokens_days > 0 {
            let cutoff = now - ChronoDuration::days(retention.email_tokens_days as i64);
            report.email_tokens = tx.execute(
                "DELETE FROM email_tokens WHERE matched_at < ?1",
                params![to_rfc3339(cutoff)],
            )?;
        }

        if retention.seen_tags_days > 0 {
            let cutoff = now - ChronoDuration::days(retention.seen_tags_days as i64);
            report.seen_tags = tx.execute(
                "DELETE FROM seen_tags WHERE seen_at < ?1",
                params![to_rfc3339(cutoff)],
            )?;
        }

        if retention.events_days > 0 {
            let cutoff = now - ChronoDuration::days(retention.events_days as i64);
            report.events = tx.execute(
                "DELETE FROM events WHERE created_at < ?1",
                params![to_rfc3339(cutoff)],
            )?;
        }

        if retention.terminal_jobs_days > 0 {
            let cutoff = now - ChronoDuration::days(retention.terminal_jobs_days as i64);
            let mut stmt = tx.prepare(
                r#"
                SELECT id
                FROM jobs
                WHERE status IN (?1, ?2, ?3, ?4)
                  AND updated_at < ?5
                "#,
            )?;
            let ids_iter = stmt.query_map(
                params![
                    JobStatus::Completed.as_str(),
                    JobStatus::Failed.as_str(),
                    JobStatus::FailedNeedsManual.as_str(),
                    JobStatus::Timeout.as_str(),
                    to_rfc3339(cutoff),
                ],
                |row| row.get::<_, String>(0),
            )?;
            let mut job_ids = Vec::new();
            for id in ids_iter {
                job_ids.push(id?);
            }
            drop(stmt);

            for chunk in job_ids.chunks(500) {
                let placeholders = chunk
                    .iter()
                    .enumerate()
                    .map(|(i, _)| format!("?{}", i + 1))
                    .collect::<Vec<_>>()
                    .join(", ");
                report.reviews += tx.execute(
                    &format!("DELETE FROM reviews WHERE job_id IN ({placeholders})"),
                    params_from_iter(chunk.iter()),
                )?;
                report.events += tx.execute(
                    &format!("DELETE FROM events WHERE job_id IN ({placeholders})"),
                    params_from_iter(chunk.iter()),
                )?;
                report.jobs += tx.execute(
                    &format!("DELETE FROM jobs WHERE id IN ({placeholders})"),
                    params_from_iter(chunk.iter()),
                )?;
            }
        }

        tx.commit()?;
        Ok(report)
    }

    /// Returns active (QUEUED, SUBMITTED, PROCESSING) jobs for a project, oldest first.
    pub fn list_active_jobs_for_project(&self, project_id: &str) -> Result<Vec<Job>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(
            r#"
            SELECT *
            FROM jobs
            WHERE project_id = ?1
              AND status IN (?2, ?3, ?4)
            ORDER BY created_at ASC
            "#,
        )?;
        let rows = stmt.query_map(
            params![
                project_id,
                JobStatus::Queued.as_str(),
                JobStatus::Submitted.as_str(),
                JobStatus::Processing.as_str(),
            ],
            map_job_row,
        )?;
        collect_rows(rows)
    }

    /// Returns recently-failed jobs for a project (status in `Failed`, `FailedNeedsManual`,
    /// `Timeout`), ordered by `updated_at DESC` so the most recent failure appears first.
    /// At most `limit` rows are returned to prevent menu blowup.
    pub fn list_failed_jobs_for_project(&self, project_id: &str, limit: usize) -> Result<Vec<Job>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(
            r#"
            SELECT *
            FROM jobs
            WHERE project_id = ?1
              AND status IN (?2, ?3, ?4)
              AND (last_error IS NULL OR last_error NOT LIKE 'cancelled by user:%')
            ORDER BY updated_at DESC
            LIMIT ?5
            "#,
        )?;
        let rows = stmt.query_map(
            params![
                project_id,
                JobStatus::Failed.as_str(),
                JobStatus::FailedNeedsManual.as_str(),
                JobStatus::Timeout.as_str(),
                limit as i64,
            ],
            map_job_row,
        )?;
        collect_rows(rows)
    }

    /// Returns the `created_at` timestamp of the most recent event for a project.
    /// Used as a proxy for "last daemon tick time" since no explicit tick events are stored.
    pub fn most_recent_event_created_at(&self, project_id: &str) -> Result<Option<DateTime<Utc>>> {
        let conn = self.connect()?;
        let ts: Option<String> = conn
            .query_row(
                "SELECT created_at FROM events WHERE project_id = ?1 ORDER BY created_at DESC, id DESC LIMIT 1",
                params![project_id],
                |row| row.get(0),
            )
            .optional()?;
        ts.as_deref()
            .map(parse_rfc3339)
            .transpose()
            .context("invalid created_at in events table")
    }

    /// Returns the most recent event of a specific type for a project.
    /// Used by `daemon status` to surface the last `tick_failed` event so
    /// operators can see when (and why) the daemon last died.
    pub fn most_recent_event_of_type(
        &self,
        project_id: &str,
        event_type: &str,
    ) -> Result<Option<EventRecord>> {
        let conn = self.connect()?;
        let row: Option<(i64, Option<String>, String, String)> = conn
            .query_row(
                r#"
                SELECT id, job_id, payload_json, created_at
                FROM events
                WHERE project_id = ?1 AND event_type = ?2
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                "#,
                params![project_id, event_type],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .optional()?;
        let Some((id, job_id, payload_json, created_at)) = row else {
            return Ok(None);
        };
        let payload: Value = serde_json::from_str(&payload_json)
            .with_context(|| format!("invalid payload_json on event id={id}"))?;
        let created_at = parse_rfc3339(&created_at)
            .with_context(|| format!("invalid created_at on event id={id}"))?;
        Ok(Some(EventRecord {
            id,
            project_id: project_id.to_string(),
            job_id,
            event_type: event_type.to_string(),
            payload,
            created_at,
        }))
    }

    /// Returns up to `limit` most-recent events of `event_type` for a project,
    /// ordered newest first.  Used by `daemon status` to surface proxy failover
    /// health without a full table scan.
    pub fn list_recent_events_of_type(
        &self,
        project_id: &str,
        event_type: &str,
        limit: usize,
    ) -> Result<Vec<EventRecord>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(
            r#"
            SELECT id, project_id, job_id, event_type, payload_json, created_at
            FROM events
            WHERE project_id = ?1 AND event_type = ?2
            ORDER BY created_at DESC, id DESC
            LIMIT ?3
            "#,
        )?;
        let rows = stmt.query_map(params![project_id, event_type, limit as i64], map_event_row)?;
        collect_rows(rows)
    }

    /// Count COMPLETED jobs whose `updated_at` starts with `date_prefix` (e.g. `"2026-05-05"`).
    /// Used by the widget state builder for `summary.completed_today`.
    /// The date is compared against the UTC date stored in `updated_at`.
    pub fn count_completed_today(&self, project_id: &str, date_prefix: &str) -> Result<usize> {
        let conn = self.connect()?;
        let count: i64 = conn.query_row(
            r#"
            SELECT COUNT(*)
            FROM jobs
            WHERE project_id = ?1
              AND status = ?2
              AND updated_at LIKE ?3
            "#,
            params![
                project_id,
                JobStatus::Completed.as_str(),
                format!("{date_prefix}%"),
            ],
            |row| row.get(0),
        )?;
        Ok(count as usize)
    }

    pub fn status_counts(&self, project_id: &str) -> Result<BTreeMap<String, usize>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(
            r#"
            SELECT status, COUNT(*) as cnt
            FROM jobs
            WHERE project_id = ?1
            GROUP BY status
            "#,
        )?;
        let mut rows = stmt.query(params![project_id])?;
        let mut out = BTreeMap::new();
        while let Some(row) = rows.next()? {
            let status: String = row.get(0)?;
            let cnt: i64 = row.get(1)?;
            out.insert(status, cnt as usize);
        }
        Ok(out)
    }
}

fn determine_versioning(conn: &Connection, new_job: &NewJob) -> Result<(u32, u32, String, String)> {
    let version_key = new_job
        .git_commit
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| new_job.pdf_hash.clone());
    let version_source = if new_job
        .git_commit
        .as_deref()
        .map(str::trim)
        .is_some_and(|value| !value.is_empty())
    {
        "git_commit".to_string()
    } else {
        "pdf_hash".to_string()
    };

    let latest: Option<(u32, String)> = conn
        .query_row(
            r#"
            SELECT version_no, version_key
            FROM jobs
            WHERE project_id = ?1 AND paper_id = ?2
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            "#,
            params![new_job.project_id, new_job.paper_id],
            |row| Ok((row.get::<_, i64>(0)? as u32, row.get::<_, String>(1)?)),
        )
        .optional()?;

    let version_no = if let Some((latest_version_no, latest_version_key)) = latest {
        if latest_version_key == version_key {
            latest_version_no
        } else {
            conn.query_row(
                "SELECT COALESCE(MAX(version_no), 0) + 1 FROM jobs WHERE project_id = ?1 AND paper_id = ?2",
                params![new_job.project_id, new_job.paper_id],
                |row| Ok(row.get::<_, i64>(0)? as u32),
            )?
        }
    } else {
        1
    };

    let completed_round_max: Option<u32> = conn
        .query_row(
            r#"
            SELECT MAX(round_no)
            FROM jobs
            WHERE project_id = ?1
              AND paper_id = ?2
              AND version_no = ?3
              AND status = ?4
            "#,
            params![
                new_job.project_id,
                new_job.paper_id,
                version_no as i64,
                JobStatus::Completed.as_str()
            ],
            |row| Ok(row.get::<_, Option<i64>>(0)?.map(|value| value as u32)),
        )
        .optional()?
        .flatten();
    let round_no = completed_round_max.unwrap_or(0) + 1;

    Ok((version_no, round_no, version_source, version_key))
}

fn collect_rows<T, F>(rows: rusqlite::MappedRows<'_, F>) -> Result<Vec<T>>
where
    F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
{
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

fn map_job_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Job> {
    let status: String = row.get("status")?;
    let started_at: Option<String> = row.get("started_at")?;
    let next_poll_at: Option<String> = row.get("next_poll_at")?;
    let created_at: String = row.get("created_at")?;
    let updated_at: String = row.get("updated_at")?;

    let status = JobStatus::from_db(&status)
        .ok_or_else(|| conversion_error(format!("invalid status: {status}")))?;

    let next_poll_at = next_poll_at
        .map(|v| parse_rfc3339(&v))
        .transpose()
        .map_err(|e| conversion_error(e.to_string()))?;

    let started_at = started_at
        .map(|v| parse_rfc3339(&v))
        .transpose()
        .map_err(|e| conversion_error(e.to_string()))?;

    let created_at = parse_rfc3339(&created_at).map_err(|e| conversion_error(e.to_string()))?;
    let updated_at = parse_rfc3339(&updated_at).map_err(|e| conversion_error(e.to_string()))?;

    Ok(Job {
        id: row.get("id")?,
        project_id: row.get("project_id")?,
        paper_id: row.get("paper_id")?,
        backend: row.get("backend")?,
        pdf_path: row.get("pdf_path")?,
        pdf_hash: row.get("pdf_hash")?,
        status,
        token: row.get("token")?,
        email: row.get("email")?,
        venue: row.get("venue")?,
        git_tag: row.get("git_tag")?,
        git_commit: row.get("git_commit")?,
        version_no: row.get::<_, i64>("version_no")? as u32,
        round_no: row.get::<_, i64>("round_no")? as u32,
        version_source: row.get("version_source")?,
        version_key: row.get("version_key")?,
        attempt: row.get::<_, i64>("attempt")? as u32,
        started_at,
        next_poll_at,
        last_error: row.get("last_error")?,
        fallback_used: row.get::<_, i64>("fallback_used")? == 1,
        created_at,
        updated_at,
    })
}

fn map_status_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<StatusView> {
    let created_at: String = row.get("created_at")?;
    let started_at: Option<String> = row.get("started_at")?;
    let next_poll_at: Option<String> = row.get("next_poll_at")?;
    let updated_at: String = row.get("updated_at")?;
    let completed_at: Option<String> = row.get("completed_at")?;
    let raw_json: Option<String> = row.get("raw_json")?;

    Ok(StatusView {
        id: row.get("id")?,
        project_id: row.get("project_id")?,
        paper_id: row.get("paper_id")?,
        backend: row.get("backend")?,
        status: row.get("status")?,
        token: row.get("token")?,
        attempt: row.get::<_, i64>("attempt")? as u32,
        created_at: parse_rfc3339(&created_at).map_err(|e| conversion_error(e.to_string()))?,
        started_at: started_at
            .map(|value| parse_rfc3339(&value))
            .transpose()
            .map_err(|e| conversion_error(e.to_string()))?,
        next_poll_at: next_poll_at
            .map(|value| parse_rfc3339(&value))
            .transpose()
            .map_err(|e| conversion_error(e.to_string()))?,
        updated_at: parse_rfc3339(&updated_at).map_err(|e| conversion_error(e.to_string()))?,
        last_error: row.get("last_error")?,
        pdf_hash: row.get("pdf_hash")?,
        git_tag: row.get("git_tag")?,
        git_commit: row.get("git_commit")?,
        version_no: row.get::<_, i64>("version_no")? as u32,
        round_no: row.get::<_, i64>("round_no")? as u32,
        version_source: row.get("version_source")?,
        version_key: row.get("version_key")?,
        score: extract_score(&raw_json),
        summary_md: row.get("summary_md")?,
        completed_at: completed_at
            .map(|value| parse_rfc3339(&value))
            .transpose()
            .map_err(|e| conversion_error(e.to_string()))?,
    })
}

fn map_event_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<EventRecord> {
    let payload_json: String = row.get("payload_json")?;
    let created_at: String = row.get("created_at")?;
    let payload = serde_json::from_str::<Value>(&payload_json)
        .map_err(|err| conversion_error(err.to_string()))?;
    Ok(EventRecord {
        id: row.get("id")?,
        project_id: row.get("project_id")?,
        job_id: row.get("job_id")?,
        event_type: row.get("event_type")?,
        payload,
        created_at: parse_rfc3339(&created_at).map_err(|e| conversion_error(e.to_string()))?,
    })
}

fn extract_score(raw_json: &Option<String>) -> Option<String> {
    let raw = raw_json.as_deref()?;
    let parsed: Value = serde_json::from_str(raw).ok()?;
    let score = parsed.get("numerical_score")?;
    match score {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

fn ensure_column_exists(
    conn: &Connection,
    table: &str,
    column: &str,
    column_def: &str,
) -> Result<()> {
    let pragma = format!("PRAGMA table_info({table})");
    let mut stmt = conn.prepare(&pragma)?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
    for row in rows {
        if row? == column {
            return Ok(());
        }
    }
    let alter = format!("ALTER TABLE {table} ADD COLUMN {column} {column_def}");
    conn.execute(&alter, [])?;
    Ok(())
}

fn conversion_error(message: String) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            message,
        )),
    )
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{JobStatus, NewJob};
    use tempfile::tempdir;

    fn make_queued_job(project_id: &str, paper_id: &str) -> NewJob {
        NewJob {
            project_id: project_id.to_string(),
            paper_id: paper_id.to_string(),
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

    /// Verify that WAL journal mode is enabled after init_schema.
    ///
    /// With WAL enabled, concurrent writes from separate connections (e.g.
    /// update_job_state holding a transaction while emit_failover_event opens
    /// a fresh connection) are retried rather than failing immediately, fixing
    /// the "failover events silently dropped under load" bug (N2).
    #[test]
    fn wal_mode_enabled_after_init_schema() {
        let tmp = tempdir().unwrap();
        let db = Db::new(tmp.path());
        db.init_schema().expect("init_schema must succeed");

        let conn = db.connect().expect("connect after init_schema");
        let mode: String = conn
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .expect("PRAGMA journal_mode must return a row");
        assert_eq!(
            mode, "wal",
            "expected WAL journal mode after init_schema; got: {mode}"
        );
    }

    /// Regression: upgrading from a pre-Phase-0 schema (jobs table without
    /// the project_id column) used to fail in init_schema because CREATE
    /// INDEX on (project_id, ...) ran BEFORE ensure_column_exists added the
    /// missing column. Reproduces the production breakage by hand-crafting
    /// an old-shape jobs table, then runs init_schema and asserts indexes
    /// got created and old rows are still readable.
    #[test]
    fn init_schema_migrates_pre_project_id_table() {
        let tmp = tempdir().unwrap();
        let db_path = tmp.path().join("legacy.db");

        // Hand-craft the pre-Phase-0 schema (no project_id, no version_*,
        // no started_at). This mirrors what a v0.1.x install left on disk.
        {
            let conn = rusqlite::Connection::open(&db_path).unwrap();
            conn.execute_batch(
                r#"
                CREATE TABLE jobs (
                    id TEXT PRIMARY KEY,
                    paper_id TEXT NOT NULL,
                    backend TEXT NOT NULL,
                    pdf_path TEXT NOT NULL,
                    pdf_hash TEXT NOT NULL,
                    status TEXT NOT NULL,
                    token TEXT,
                    email TEXT NOT NULL,
                    venue TEXT,
                    git_tag TEXT,
                    git_commit TEXT,
                    attempt INTEGER NOT NULL DEFAULT 0,
                    next_poll_at TEXT,
                    last_error TEXT,
                    fallback_used INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE seen_tags (
                    tag_name TEXT PRIMARY KEY,
                    target_commit TEXT NOT NULL,
                    seen_at TEXT NOT NULL
                );
                CREATE TABLE email_tokens (
                    token TEXT PRIMARY KEY,
                    source TEXT NOT NULL,
                    matched_at TEXT NOT NULL,
                    raw_ref TEXT
                );
                CREATE TABLE reviews (
                    job_id TEXT PRIMARY KEY,
                    token TEXT NOT NULL,
                    raw_json TEXT NOT NULL,
                    summary_md TEXT NOT NULL,
                    completed_at TEXT NOT NULL
                );
                INSERT INTO jobs (id, paper_id, backend, pdf_path, pdf_hash, status, email, attempt, fallback_used, created_at, updated_at)
                VALUES ('legacy-job-1', 'main', 'stanford', '/tmp/p.pdf', 'h1', 'COMPLETED', 'test@example.com', 0, 0, '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z');
                "#,
            ).unwrap();
        }

        // Now run init_schema -- this used to fail with "no such column:
        // project_id" because CREATE INDEX ran before ensure_column_exists.
        let db = Db::new_file(db_path.clone());
        db.init_schema()
            .expect("init_schema must succeed on legacy db");

        // Old row is still there and the new column got a default.
        let conn = db.connect().unwrap();
        let (proj, paper_id): (String, String) = conn
            .query_row(
                "SELECT project_id, paper_id FROM jobs WHERE id = ?1",
                ["legacy-job-1"],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("legacy row preserved");
        assert_eq!(proj, "");
        assert_eq!(paper_id, "main");

        // Indexes referencing project_id were actually created.
        let idx_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'idx_jobs_project_status_next_poll'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(idx_count, 1, "project_id-prefixed index must exist");
    }

    #[test]
    fn update_job_state_rejects_terminal_to_active_transition() {
        let db = Db::new_in_memory("guard_test").unwrap();
        db.init_schema().unwrap();

        let job = db.create_job(&make_queued_job("proj", "p1")).unwrap();
        // Move to a terminal state via the unchecked path.
        db.update_job_state_unchecked(&job.id, JobStatus::Completed, None, Some(None), None)
            .unwrap();

        // The checked path must reject Completed -> Queued.
        let err = db
            .update_job_state(&job.id, JobStatus::Queued, None, Some(None), None)
            .unwrap_err();
        assert!(
            err.to_string().contains("invalid status transition"),
            "expected 'invalid status transition', got: {err}"
        );
    }

    #[test]
    fn update_job_state_allows_valid_worker_transitions() {
        let db = Db::new_in_memory("guard_valid_test").unwrap();
        db.init_schema().unwrap();

        let job = db.create_job(&make_queued_job("proj", "p2")).unwrap();
        // Queued -> Processing is a valid worker transition.
        db.update_job_state(&job.id, JobStatus::Processing, None, Some(None), None)
            .unwrap();
        // Processing -> Completed is valid.
        db.update_job_state(&job.id, JobStatus::Completed, None, Some(None), None)
            .unwrap();
    }
}
