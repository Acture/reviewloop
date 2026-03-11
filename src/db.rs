use crate::{
    config::Config,
    model::{EventRecord, Job, JobStatus, NewJob, StatusView},
    util::{parse_rfc3339, to_rfc3339},
};
use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
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
        let conn = Connection::open_with_flags(&self.dsn, self.open_flags)
            .with_context(|| format!("failed to open sqlite database: {}", self.dsn))?;
        conn.busy_timeout(Duration::from_secs(5))?;
        Ok(conn)
    }

    pub fn init_schema(&self) -> Result<()> {
        let conn = self.connect()?;
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

            CREATE INDEX IF NOT EXISTS idx_jobs_project_status_next_poll ON jobs(project_id, status, next_poll_at);
            CREATE INDEX IF NOT EXISTS idx_jobs_project_backend_hash ON jobs(project_id, backend, pdf_hash);
            CREATE INDEX IF NOT EXISTS idx_jobs_project_paper_backend ON jobs(project_id, paper_id, backend);
            CREATE INDEX IF NOT EXISTS idx_jobs_project_dedupe ON jobs(project_id, paper_id, backend, pdf_hash, version_key, status);
            CREATE INDEX IF NOT EXISTS idx_events_project_created_at ON events(project_id, created_at);
            "#,
        )?;
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
        let mut job_ids = BTreeSet::new();
        {
            let mut stmt = conn.prepare(
                "SELECT id FROM jobs WHERE project_id = ?1 AND paper_id = ?2 ORDER BY created_at ASC",
            )?;
            let rows =
                stmt.query_map(params![project_id, paper_id], |row| row.get::<_, String>(0))?;
            for row in rows {
                job_ids.insert(row?);
            }
        }

        let mut events = Vec::new();
        let mut stmt = conn.prepare(
            r#"
            SELECT id, project_id, job_id, event_type, payload_json, created_at
            FROM events
            WHERE project_id = ?1
            ORDER BY created_at ASC, id ASC
            "#,
        )?;
        let rows = stmt.query_map(params![project_id], map_event_row)?;
        for row in rows {
            let event = row?;
            let matches_job = event
                .job_id
                .as_deref()
                .is_some_and(|job_id| job_ids.contains(job_id));
            let matches_payload = event
                .payload
                .get("paper_id")
                .and_then(Value::as_str)
                .is_some_and(|value| value == paper_id);
            if matches_job || matches_payload {
                events.push(event);
            }
        }
        Ok(events)
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

    pub fn update_job_state(
        &self,
        job_id: &str,
        status: JobStatus,
        attempt: Option<u32>,
        next_poll_at: Option<Option<DateTime<Utc>>>,
        last_error: Option<Option<String>>,
    ) -> Result<()> {
        let conn = self.connect()?;
        let current = self
            .get_job(job_id)?
            .ok_or_else(|| anyhow!("job not found: {job_id}"))?;

        let attempt_val = attempt.unwrap_or(current.attempt);
        let next_poll_val = next_poll_at.unwrap_or(current.next_poll_at).map(to_rfc3339);
        let last_error_val = last_error.unwrap_or(current.last_error);

        conn.execute(
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
        Ok(())
    }

    pub fn mark_submitted_with_token(
        &self,
        job_id: &str,
        token: &str,
        next_poll_at: DateTime<Utc>,
    ) -> Result<()> {
        let conn = self.connect()?;
        let now = to_rfc3339(Utc::now());
        conn.execute(
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

            for id in job_ids {
                report.reviews +=
                    tx.execute("DELETE FROM reviews WHERE job_id = ?1", params![&id])?;
                report.events +=
                    tx.execute("DELETE FROM events WHERE job_id = ?1", params![&id])?;
                report.jobs += tx.execute("DELETE FROM jobs WHERE id = ?1", params![&id])?;
            }
        }

        tx.commit()?;
        Ok(report)
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
