use crate::{
    model::{Job, JobStatus, NewJob, StatusView},
    util::{parse_rfc3339, to_rfc3339},
};
use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Db {
    pub path: PathBuf,
}

impl Db {
    pub fn new(state_dir: &Path) -> Self {
        Self {
            path: state_dir.join("reviewloop.db"),
        }
    }

    fn connect(&self) -> Result<Connection> {
        let conn = Connection::open(&self.path)
            .with_context(|| format!("failed to open sqlite database: {}", self.path.display()))?;
        conn.busy_timeout(Duration::from_secs(5))?;
        Ok(conn)
    }

    pub fn init_schema(&self) -> Result<()> {
        let conn = self.connect()?;
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS jobs (
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

            CREATE INDEX IF NOT EXISTS idx_jobs_status_next_poll ON jobs(status, next_poll_at);
            CREATE INDEX IF NOT EXISTS idx_jobs_backend_hash ON jobs(backend, pdf_hash);
            CREATE INDEX IF NOT EXISTS idx_jobs_paper_backend ON jobs(paper_id, backend);
            "#,
        )?;
        Ok(())
    }

    pub fn create_job(&self, new_job: &NewJob) -> Result<Job> {
        let now = Utc::now();
        let id = Uuid::new_v4().to_string();
        let conn = self.connect()?;

        conn.execute(
            r#"
            INSERT INTO jobs (
                id, paper_id, backend, pdf_path, pdf_hash, status, token, email, venue,
                git_tag, git_commit, attempt, next_poll_at, last_error, fallback_used,
                created_at, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, ?8, ?9, ?10, 0, ?11, NULL, 0, ?12, ?12)
            "#,
            params![
                id,
                new_job.paper_id,
                new_job.backend,
                new_job.pdf_path,
                new_job.pdf_hash,
                new_job.status.as_str(),
                new_job.email,
                new_job.venue,
                new_job.git_tag,
                new_job.git_commit,
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

    pub fn list_status_views(&self, paper_id: Option<&str>) -> Result<Vec<StatusView>> {
        let conn = self.connect()?;
        let mut out = Vec::new();

        if let Some(paper_id) = paper_id {
            let mut stmt = conn.prepare(
                r#"
                SELECT id, paper_id, backend, status, token, attempt, next_poll_at, updated_at, last_error
                FROM jobs
                WHERE paper_id = ?1
                ORDER BY created_at DESC
                LIMIT 100
                "#,
            )?;
            let rows = stmt.query_map(params![paper_id], map_status_row)?;
            for row in rows {
                out.push(row?);
            }
        } else {
            let mut stmt = conn.prepare(
                r#"
                SELECT id, paper_id, backend, status, token, attempt, next_poll_at, updated_at, last_error
                FROM jobs
                ORDER BY created_at DESC
                LIMIT 100
                "#,
            )?;
            let rows = stmt.query_map([], map_status_row)?;
            for row in rows {
                out.push(row?);
            }
        }

        Ok(out)
    }

    pub fn has_duplicate_guard(&self, backend: &str, pdf_hash: &str) -> Result<bool> {
        let conn = self.connect()?;
        let statuses = [
            JobStatus::PendingApproval.as_str(),
            JobStatus::Queued.as_str(),
            JobStatus::Submitted.as_str(),
            JobStatus::Processing.as_str(),
            JobStatus::Completed.as_str(),
        ];

        let exists: Option<i64> = conn
            .query_row(
                r#"
                SELECT 1
                FROM jobs
                WHERE backend = ?1 AND pdf_hash = ?2 AND status IN (?3, ?4, ?5, ?6, ?7)
                LIMIT 1
                "#,
                params![
                    backend,
                    pdf_hash,
                    statuses[0],
                    statuses[1],
                    statuses[2],
                    statuses[3],
                    statuses[4]
                ],
                |row| row.get(0),
            )
            .optional()?;

        Ok(exists.is_some())
    }

    pub fn latest_hash_for_paper(&self, paper_id: &str, backend: &str) -> Result<Option<String>> {
        let conn = self.connect()?;
        conn.query_row(
            r#"
            SELECT pdf_hash
            FROM jobs
            WHERE paper_id = ?1 AND backend = ?2
            ORDER BY created_at DESC
            LIMIT 1
            "#,
            params![paper_id, backend],
            |row| row.get(0),
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn list_ready_queued(&self, limit: usize, now: DateTime<Utc>) -> Result<Vec<Job>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(
            r#"
            SELECT *
            FROM jobs
            WHERE status = ?1
                AND (next_poll_at IS NULL OR next_poll_at <= ?2)
            ORDER BY created_at ASC
            LIMIT ?3
            "#,
        )?;

        let rows = stmt.query_map(
            params![JobStatus::Queued.as_str(), to_rfc3339(now), limit as i64],
            map_job_row,
        )?;

        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    pub fn list_due_processing(&self, limit: usize, now: DateTime<Utc>) -> Result<Vec<Job>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(
            r#"
            SELECT *
            FROM jobs
            WHERE status = ?1
                AND token IS NOT NULL
                AND (next_poll_at IS NULL OR next_poll_at <= ?2)
            ORDER BY COALESCE(next_poll_at, created_at) ASC
            LIMIT ?3
            "#,
        )?;

        let rows = stmt.query_map(
            params![
                JobStatus::Processing.as_str(),
                to_rfc3339(now),
                limit as i64
            ],
            map_job_row,
        )?;

        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    pub fn count_active_submit_poll(&self) -> Result<usize> {
        let conn = self.connect()?;
        let count: i64 = conn.query_row(
            r#"
            SELECT COUNT(*)
            FROM jobs
            WHERE status IN (?1, ?2)
            "#,
            params![
                JobStatus::Submitted.as_str(),
                JobStatus::Processing.as_str()
            ],
            |row| row.get(0),
        )?;
        Ok(count as usize)
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
        conn.execute(
            r#"
            UPDATE jobs
            SET status = ?2,
                token = ?3,
                next_poll_at = ?4,
                last_error = NULL,
                attempt = 0,
                updated_at = ?5
            WHERE id = ?1
            "#,
            params![
                job_id,
                JobStatus::Processing.as_str(),
                token,
                to_rfc3339(next_poll_at),
                to_rfc3339(Utc::now()),
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

    pub fn add_event(&self, job_id: Option<&str>, event_type: &str, payload: Value) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            r#"
            INSERT INTO events(job_id, event_type, payload_json, created_at)
            VALUES (?1, ?2, ?3, ?4)
            "#,
            params![
                job_id,
                event_type,
                payload.to_string(),
                to_rfc3339(Utc::now())
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

    pub fn find_latest_open_job_for_paper(&self, paper_id: &str) -> Result<Option<Job>> {
        let conn = self.connect()?;
        conn.query_row(
            r#"
            SELECT *
            FROM jobs
            WHERE paper_id = ?1
              AND status NOT IN (?2, ?3, ?4, ?5)
            ORDER BY created_at DESC
            LIMIT 1
            "#,
            params![
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

    pub fn find_latest_open_job_without_token(&self, backend: &str) -> Result<Option<Job>> {
        let conn = self.connect()?;
        conn.query_row(
            r#"
            SELECT *
            FROM jobs
            WHERE backend = ?1
              AND token IS NULL
              AND status IN (?2, ?3, ?4, ?5)
            ORDER BY created_at DESC
            LIMIT 1
            "#,
            params![
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

    pub fn attach_token_to_job(
        &self,
        job_id: &str,
        token: &str,
        next_poll_at: DateTime<Utc>,
    ) -> Result<()> {
        self.mark_submitted_with_token(job_id, token, next_poll_at)
    }

    pub fn list_processing_jobs(&self) -> Result<Vec<Job>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare("SELECT * FROM jobs WHERE status = ?1")?;
        let rows = stmt.query_map(params![JobStatus::Processing.as_str()], map_job_row)?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
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
}

fn map_job_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Job> {
    let status: String = row.get("status")?;
    let next_poll_at: Option<String> = row.get("next_poll_at")?;
    let created_at: String = row.get("created_at")?;
    let updated_at: String = row.get("updated_at")?;

    let status = JobStatus::from_db(&status)
        .ok_or_else(|| conversion_error(format!("invalid status: {status}")))?;

    let next_poll_at = next_poll_at
        .map(|v| parse_rfc3339(&v))
        .transpose()
        .map_err(|e| conversion_error(e.to_string()))?;

    let created_at = parse_rfc3339(&created_at).map_err(|e| conversion_error(e.to_string()))?;
    let updated_at = parse_rfc3339(&updated_at).map_err(|e| conversion_error(e.to_string()))?;

    Ok(Job {
        id: row.get("id")?,
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
        attempt: row.get::<_, i64>("attempt")? as u32,
        next_poll_at,
        last_error: row.get("last_error")?,
        fallback_used: row.get::<_, i64>("fallback_used")? == 1,
        created_at,
        updated_at,
    })
}

fn map_status_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<StatusView> {
    let status: String = row.get("status")?;
    let next_poll_at: Option<String> = row.get("next_poll_at")?;
    let updated_at: String = row.get("updated_at")?;

    Ok(StatusView {
        id: row.get("id")?,
        paper_id: row.get("paper_id")?,
        backend: row.get("backend")?,
        status,
        token: row.get("token")?,
        attempt: row.get::<_, i64>("attempt")? as u32,
        next_poll_at: next_poll_at
            .map(|v| parse_rfc3339(&v))
            .transpose()
            .map_err(|e| conversion_error(e.to_string()))?,
        updated_at: parse_rfc3339(&updated_at).map_err(|e| conversion_error(e.to_string()))?,
        last_error: row.get("last_error")?,
    })
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
