use crate::{
    config::Config,
    model::{
        EnqueueConflict, EnqueueMode, EnqueueOutcome, EnqueueRequest, EventRecord, ExistingReason,
        Job, JobStatus, NewJob, ProviderUsage, RegisteredProject, ReviewIdentity, ReviewOptions,
        ReviewRecord, StatusView, SubmitChannel, SubmitStage, WorkKind,
    },
    util::{parse_rfc3339, to_rfc3339},
};
use anyhow::{Context, Result, anyhow, ensure};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use rusqlite::{
    Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior, params,
    params_from_iter,
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashSet},
    path::{Path, PathBuf},
    time::Duration,
};
use uuid::Uuid;

const SCHEMA_VERSION: u32 = 5;

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

/// Exclusive, time-bounded right to act on one job, obtained from [`Db::claim_job`].
/// Writes made on its behalf are rejected once `expires_at` passes or the lease is
/// revoked (cancel, user override, external token attach).
#[derive(Debug, Clone)]
pub struct Lease {
    /// The job as of the latest write made under this lease.
    pub job: Job,
    pub kind: WorkKind,
    pub owner: String,
    pub expires_at: DateTime<Utc>,
}

/// Whether a claim honours the job's `next_poll_at` cooldown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimTiming {
    /// Daemon scheduling: claim only once `next_poll_at` has passed.
    WhenDue,
    /// Explicit CLI action: claim regardless of cooldown.
    Now,
}

/// Outcome of a write guarded by lease ownership.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseWrite {
    Applied,
    /// The lease expired or was revoked, so nothing was written. Carries the job's
    /// current status (`None` when the row is gone).
    Lost(Option<JobStatus>),
}

/// State a lease owner leaves the job in; finishing always releases the lease.
#[derive(Debug, Clone)]
pub struct JobChange {
    pub status: JobStatus,
    pub attempt: Option<u32>,
    pub next_poll_at: Option<Option<DateTime<Utc>>>,
    pub last_error: Option<Option<String>>,
    /// Stage left on the row; `None` clears it.
    pub submit_stage: Option<SubmitStage>,
    /// New `fallback_used` flag; `None` keeps it.
    pub fallback_used: Option<bool>,
}

#[derive(Debug, Clone, Copy)]
pub struct NewReview<'a> {
    pub token: &'a str,
    pub raw_json: &'a str,
    pub summary_md: &'a str,
}

/// What became of a submit receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiptWrite {
    /// Recorded by the lease owner; the job is PROCESSING.
    Accepted,
    /// The lease was lost, but the job had no token and no live owner, so the token
    /// was stored for recovery without changing the job's status.
    StoredForRecovery,
    /// The lease was lost and the job could not take the token; only an event records it.
    Logged,
}

/// Outcome of [`Db::requeue`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Requeue {
    Requeued,
    /// A worker is sending this job's submission right now.
    InFlight {
        owner: String,
        expires_at: DateTime<Utc>,
    },
    /// The job already has a receipt token; polling it, not resubmitting, is the retry.
    HasReceipt,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct LeaseRecovery {
    /// Expired pre-dispatch submit claims returned to the queue.
    pub released_claims: usize,
    /// SUBMITTED jobs without a live owner, now marked pending reconciliation.
    pub uncertain_submits: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelOutcome {
    Cancelled {
        previous_status: JobStatus,
        previous_stage: Option<SubmitStage>,
        /// A worker held a live lease, so a request may still be in flight.
        lease_was_active: bool,
    },
    AlreadyTerminal(JobStatus),
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
                    if let Err(e) =
                        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
                    {
                        tracing::warn!(
                            path = %path.display(),
                            error = %e,
                            "failed to enforce 0o600 on database file; credentials may be world-readable"
                        );
                    }
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
        // ensure_schema) further reduces contention, but having a generous
        // timeout is a belt-and-suspenders safeguard.
        conn.busy_timeout(Duration::from_secs(30))?;
        Ok(conn)
    }

    pub fn ensure_schema(&self) -> Result<()> {
        let mut conn = self.connect()?;
        enable_wal_mode(&conn).context("enabling WAL mode")?;
        if schema_version(&conn)? >= SCHEMA_VERSION {
            return Ok(());
        }

        // A daemon and a CLI call can start together; the write lock serializes their
        // migrations and the re-check makes the loser a no-op.
        let tx = begin_immediate(&mut conn)?;
        if schema_version(&tx)? >= SCHEMA_VERSION {
            return Ok(());
        }
        create_tables_if_missing(&tx).context("creating tables")?;
        migrate_columns(&tx).context("migrating columns")?;
        create_indexes(&tx).context("creating indexes")?;
        tx.pragma_update(None, "user_version", SCHEMA_VERSION as i64)
            .context("recording schema version")?;
        tx.commit()?;
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

    /// Insert a job unconditionally: no request-key lookup, no coverage check,
    /// no event. Version and round are allocated exactly as [`Db::enqueue`]
    /// allocates them. Entry points that accept review requests use `enqueue`.
    pub fn create_job(&self, new_job: &NewJob) -> Result<Job> {
        let mut conn = self.connect()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let job = insert_job(&tx, new_job, &new_job.review_identity())?;
        tx.commit()?;
        Ok(job)
    }

    /// Enqueue a review request, or return the job that already answers it.
    ///
    /// One `BEGIN IMMEDIATE` transaction resolves the request key, checks
    /// coverage, allocates version and round, inserts the job, binds the key
    /// and writes the enqueue event, so concurrent callers on separate
    /// connections or processes serialize here and never both create a job.
    /// A key replayed with different content fails with [`EnqueueConflict`]
    /// and writes nothing.
    pub fn enqueue(&self, request: &EnqueueRequest) -> Result<EnqueueOutcome> {
        if let Some(key) = &request.request_key {
            ensure!(!key.trim().is_empty(), "request key must not be blank");
        }
        let mut conn = self.connect()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let outcome = enqueue_in_tx(&tx, request)?;
        tx.commit()?;
        Ok(outcome)
    }

    pub fn get_job(&self, job_id: &str) -> Result<Option<Job>> {
        load_job(&self.connect()?, job_id)
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

    /// The newest pending, in-flight or completed job with this identity.
    pub fn find_duplicate_covering_job(
        &self,
        project_id: &str,
        identity: &ReviewIdentity,
    ) -> Result<Option<Job>> {
        find_covering_job(&self.connect()?, project_id, identity)
    }

    /// Record that a request was skipped because `existing` already covers it.
    /// [`Db::enqueue`] records this itself; this is for callers that check
    /// coverage before preparing a request.
    pub fn record_duplicate_skip(
        &self,
        project_id: &str,
        identity: &ReviewIdentity,
        existing: &Job,
        source: &str,
    ) -> Result<()> {
        insert_duplicate_skipped(
            &self.connect()?,
            project_id,
            identity,
            existing,
            None,
            source,
        )
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
              AND (lease_owner IS NULL OR lease_expires_at IS NULL OR lease_expires_at <= ?3)
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
              AND (lease_owner IS NULL OR lease_expires_at IS NULL OR lease_expires_at <= ?3)
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

    /// Update job state, enforcing `JobStatus::can_transition` as a guard. Validation and
    /// write share one transaction. A status change revokes any work lease (the owner's
    /// premise no longer holds); a same-status bookkeeping update leaves it in place.
    /// Use [`update_job_state_unchecked`] for deliberate user overrides (retry --force,
    /// complete) that legitimately move out of terminal states.
    pub fn update_job_state(
        &self,
        job_id: &str,
        status: JobStatus,
        attempt: Option<u32>,
        next_poll_at: Option<Option<DateTime<Utc>>>,
        last_error: Option<Option<String>>,
    ) -> Result<()> {
        let mut conn = self.connect()?;
        let tx = begin_immediate(&mut conn)?;
        let current = require_job(&tx, job_id)?;
        if !current.status.can_transition(status) {
            anyhow::bail!(
                "invalid status transition for job {}: {} -> {}",
                job_id,
                current.status.as_str(),
                status.as_str()
            );
        }
        let (lease, submit_stage) = if status == current.status {
            (LeaseColumns::Keep, None)
        } else {
            (LeaseColumns::Clear, Some(None))
        };
        write_row(
            &tx,
            &current,
            RowWrite {
                attempt,
                next_poll_at,
                last_error,
                submit_stage,
                ..RowWrite::new(status, lease)
            },
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Update job state without enforcing the state-machine guard, revoking any work
    /// lease so an in-flight worker's result is rejected.
    /// Use at CLI override sites (cmd_retry --force, cmd_complete) that deliberately move
    /// jobs out of terminal or otherwise-restricted states.
    pub fn update_job_state_unchecked(
        &self,
        job_id: &str,
        status: JobStatus,
        attempt: Option<u32>,
        next_poll_at: Option<Option<DateTime<Utc>>>,
        last_error: Option<Option<String>>,
    ) -> Result<()> {
        let mut conn = self.connect()?;
        let tx = begin_immediate(&mut conn)?;
        let current = require_job(&tx, job_id)?;
        write_row(
            &tx,
            &current,
            RowWrite {
                attempt,
                next_poll_at,
                last_error,
                submit_stage: Some(None),
                ..RowWrite::new(status, LeaseColumns::Clear)
            },
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Reset a job's retry bookkeeping. Status, lease and submit stage are read and kept
    /// inside the transaction, so a caller's stale view can never become a transition.
    /// `None` fields keep the current value.
    pub fn reschedule(
        &self,
        job_id: &str,
        attempt: Option<u32>,
        next_poll_at: Option<Option<DateTime<Utc>>>,
    ) -> Result<()> {
        let mut conn = self.connect()?;
        let tx = begin_immediate(&mut conn)?;
        let current = require_job(&tx, job_id)?;
        write_row(
            &tx,
            &current,
            RowWrite {
                attempt,
                next_poll_at,
                ..RowWrite::new(current.status, LeaseColumns::Keep)
            },
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Bring a PROCESSING job's next poll forward to `at` unless it is already due
    /// sooner. Returns `false` when nothing changed.
    pub fn pull_poll_forward(&self, job_id: &str, at: DateTime<Utc>) -> Result<bool> {
        let mut conn = self.connect()?;
        let tx = begin_immediate(&mut conn)?;
        let current = require_job(&tx, job_id)?;
        // A NULL next_poll_at is already due.
        if current.status != JobStatus::Processing
            || current.next_poll_at.is_none_or(|next| next <= at)
        {
            return Ok(false);
        }
        write_row(
            &tx,
            &current,
            RowWrite {
                next_poll_at: Some(Some(at)),
                ..RowWrite::new(current.status, LeaseColumns::Keep)
            },
        )?;
        tx.commit()?;
        Ok(true)
    }

    /// Put a job back to QUEUED for another submit attempt (user `retry`): attempt 0, no
    /// cooldown or error, lease revoked. Both refusals are re-checked inside the
    /// transaction, so a caller's stale view cannot turn into a duplicate submission:
    /// a submission in flight (DISPATCHED with a live lease) or one whose receipt has
    /// landed meanwhile (the job holds a token) is left alone.
    pub fn requeue(&self, job_id: &str, now: DateTime<Utc>) -> Result<Requeue> {
        let mut conn = self.connect()?;
        let tx = begin_immediate(&mut conn)?;
        let current = require_job(&tx, job_id)?;
        if current.token.is_some() {
            return Ok(Requeue::HasReceipt);
        }
        if current.submit_stage == Some(SubmitStage::Dispatched) && lease_is_live(&current, now) {
            return Ok(Requeue::InFlight {
                owner: current.lease_owner.clone().unwrap_or_default(),
                expires_at: current.lease_expires_at.unwrap_or(now),
            });
        }
        write_row(
            &tx,
            &current,
            RowWrite {
                attempt: Some(0),
                next_poll_at: Some(None),
                last_error: Some(None),
                submit_stage: Some(None),
                ..RowWrite::new(JobStatus::Queued, LeaseColumns::Clear)
            },
        )?;
        tx.commit()?;
        Ok(Requeue::Requeued)
    }

    /// Attach a token obtained outside the worker (email ingestion, `import-token`) and
    /// move the job to PROCESSING. The token is authoritative, so any work lease is revoked.
    pub fn mark_submitted_with_token(
        &self,
        job_id: &str,
        token: &str,
        next_poll_at: DateTime<Utc>,
    ) -> Result<()> {
        let mut conn = self.connect()?;
        let tx = begin_immediate(&mut conn)?;
        let current = require_job(&tx, job_id)?;
        if !current.status.can_transition(JobStatus::Processing) {
            anyhow::bail!(
                "invalid status transition for job {}: {} -> {}",
                job_id,
                current.status.as_str(),
                JobStatus::Processing.as_str()
            );
        }
        write_receipt(&tx, &current, token, next_poll_at, None)?;
        tx.commit()?;
        Ok(())
    }

    /// Pin a backfilled snapshot to a job created before snapshots existed.
    pub fn set_job_snapshot(&self, job_id: &str, snapshot_path: &Path) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            "UPDATE jobs SET snapshot_path = ?1, updated_at = ?2 WHERE id = ?3",
            params![
                snapshot_path.to_string_lossy(),
                to_rfc3339(Utc::now()),
                job_id
            ],
        )?;
        Ok(())
    }

    /// Hashes of every job row, across all projects. Snapshot directories are
    /// keyed by hash and shared by all projects using this database, so any
    /// remaining row keeps its snapshot alive (including legacy rows that may
    /// still be backfilled).
    pub fn list_job_pdf_hashes(&self) -> Result<HashSet<String>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare("SELECT DISTINCT pdf_hash FROM jobs")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        rows.collect::<rusqlite::Result<_>>().map_err(Into::into)
    }

    /// Atomically take the job's lease for `kind` when it is claimable: QUEUED for submit,
    /// PROCESSING for poll, no live lease, and — with [`ClaimTiming::WhenDue`] — past its
    /// `next_poll_at`. Returns `None` otherwise. A submit claim marks the row CLAIMED:
    /// nothing has been sent yet, so an expired claim is safe to take over.
    pub fn claim_job(
        &self,
        job_id: &str,
        kind: WorkKind,
        timing: ClaimTiming,
        now: DateTime<Utc>,
        ttl: ChronoDuration,
    ) -> Result<Option<Lease>> {
        let mut conn = self.connect()?;
        let tx = begin_immediate(&mut conn)?;
        let job = require_job(&tx, job_id)?;
        let claimable_status = match kind {
            WorkKind::Submit => JobStatus::Queued,
            WorkKind::Poll => JobStatus::Processing,
        };
        let due = match timing {
            ClaimTiming::Now => true,
            ClaimTiming::WhenDue => job.next_poll_at.is_none_or(|at| at <= now),
        };
        if job.status != claimable_status || !due || lease_is_live(&job, now) {
            return Ok(None);
        }

        let owner = format!("{}-{}", std::process::id(), Uuid::new_v4());
        let expires_at = now + ttl;
        let submit_stage = match kind {
            WorkKind::Submit => Some(Some(SubmitStage::Claimed)),
            WorkKind::Poll => None,
        };
        write_row(
            &tx,
            &job,
            RowWrite {
                submit_stage,
                ..RowWrite::new(
                    job.status,
                    LeaseColumns::Set {
                        owner: &owner,
                        expires_at,
                    },
                )
            },
        )?;
        if kind == WorkKind::Submit
            && let Some(previous_owner) = job.lease_owner.as_deref()
        {
            insert_job_event(
                &tx,
                &job,
                "submit_claim_taken_over",
                &json!({ "previous_owner": previous_owner, "owner": owner }),
            )?;
        }
        let job = require_job(&tx, job_id)?;
        tx.commit()?;
        Ok(Some(Lease {
            job,
            kind,
            owner,
            expires_at,
        }))
    }

    /// Record that the lease owner is about to send the submission through `channel`:
    /// SUBMITTED/DISPATCHED, lease renewed to `now + ttl`, and a `submit_dispatched` event,
    /// in one transaction. Returns `false` — and the caller must not send — when the
    /// lease is no longer held.
    pub fn begin_submit_dispatch(
        &self,
        lease: &mut Lease,
        channel: SubmitChannel,
        now: DateTime<Utc>,
        ttl: ChronoDuration,
    ) -> Result<bool> {
        anyhow::ensure!(
            lease.kind == WorkKind::Submit,
            "begin_submit_dispatch needs a submit lease"
        );
        let mut conn = self.connect()?;
        let tx = begin_immediate(&mut conn)?;
        let Some(job) = load_job(&tx, &lease.job.id)? else {
            return Ok(false);
        };
        if !held_by(&job, &lease.owner, now) || !job.status.can_transition(JobStatus::Submitted) {
            return Ok(false);
        }

        let expires_at = now + ttl;
        write_row(
            &tx,
            &job,
            RowWrite {
                submit_stage: Some(Some(SubmitStage::Dispatched)),
                // Set before the script runs so a crash mid-fallback cannot rerun it;
                // cleared again if the fallback provably never reached the provider.
                fallback_used: (channel == SubmitChannel::Fallback).then_some(true),
                ..RowWrite::new(
                    JobStatus::Submitted,
                    LeaseColumns::Set {
                        owner: &lease.owner,
                        expires_at,
                    },
                )
            },
        )?;
        insert_job_event(
            &tx,
            &job,
            "submit_dispatched",
            &json!({ "channel": channel.as_str(), "owner": lease.owner }),
        )?;
        let job = require_job(&tx, &job.id)?;
        tx.commit()?;
        lease.job = job;
        lease.expires_at = expires_at;
        Ok(true)
    }

    /// Give the lease back without changing the job's status, e.g. after a local error
    /// before anything was sent. A submit lease is only released while still CLAIMED: once
    /// dispatched, its outcome must be recorded instead. Returns `false` when this owner
    /// no longer holds the row.
    pub fn release_lease(&self, lease: &Lease) -> Result<bool> {
        let mut conn = self.connect()?;
        let tx = begin_immediate(&mut conn)?;
        let Some(job) = load_job(&tx, &lease.job.id)? else {
            return Ok(false);
        };
        let releasable = match lease.kind {
            WorkKind::Submit => job.submit_stage == Some(SubmitStage::Claimed),
            WorkKind::Poll => true,
        };
        if job.lease_owner.as_deref() != Some(lease.owner.as_str()) || !releasable {
            return Ok(false);
        }
        let submit_stage = match lease.kind {
            WorkKind::Submit => Some(None),
            WorkKind::Poll => None,
        };
        write_row(
            &tx,
            &job,
            RowWrite {
                submit_stage,
                ..RowWrite::new(job.status, LeaseColumns::Clear)
            },
        )?;
        tx.commit()?;
        Ok(true)
    }

    /// Apply `change` and record the event in one transaction, releasing the lease — but
    /// only while `lease` is still held at `now`. A lost lease writes nothing.
    pub fn finish_lease(
        &self,
        lease: &Lease,
        now: DateTime<Utc>,
        change: &JobChange,
        event_type: &str,
        payload: Value,
    ) -> Result<LeaseWrite> {
        self.finish(lease, now, change, None, event_type, payload)
    }

    /// [`finish_lease`](Self::finish_lease) that also stores the fetched review.
    pub fn finish_lease_with_review(
        &self,
        lease: &Lease,
        now: DateTime<Utc>,
        review: NewReview<'_>,
        change: &JobChange,
        event_type: &str,
        payload: Value,
    ) -> Result<LeaseWrite> {
        self.finish(lease, now, change, Some(review), event_type, payload)
    }

    fn finish(
        &self,
        lease: &Lease,
        now: DateTime<Utc>,
        change: &JobChange,
        review: Option<NewReview<'_>>,
        event_type: &str,
        payload: Value,
    ) -> Result<LeaseWrite> {
        let mut conn = self.connect()?;
        let tx = begin_immediate(&mut conn)?;
        let Some(job) = load_job(&tx, &lease.job.id)? else {
            return Ok(LeaseWrite::Lost(None));
        };
        if !held_by(&job, &lease.owner, now) {
            return Ok(LeaseWrite::Lost(Some(job.status)));
        }
        anyhow::ensure!(
            job.status.can_transition(change.status),
            "invalid status transition for job {}: {} -> {}",
            job.id,
            job.status.as_str(),
            change.status.as_str()
        );
        if let Some(review) = review {
            upsert_review_row(&tx, &job.id, review)?;
        }
        write_row(
            &tx,
            &job,
            RowWrite {
                attempt: change.attempt,
                next_poll_at: change.next_poll_at,
                last_error: change.last_error.clone(),
                submit_stage: Some(change.submit_stage),
                fallback_used: change.fallback_used,
                ..RowWrite::new(change.status, LeaseColumns::Clear)
            },
        )?;
        insert_job_event(&tx, &job, event_type, &payload)?;
        tx.commit()?;
        Ok(LeaseWrite::Applied)
    }

    /// Record the provider's receipt for a dispatched submission.
    ///
    /// While the lease is held the job moves to PROCESSING with the token. A receipt
    /// arriving after the lease was lost never changes the job's status, but it is not
    /// discarded: an event always records it, and the token is stored when the job has
    /// none and no live owner (pending reconciliation, cancelled, failed) — so it can be
    /// recovered and email ingestion cannot bind it to a different job.
    pub fn record_submit_receipt(
        &self,
        lease: &Lease,
        now: DateTime<Utc>,
        token: &str,
        next_poll_at: DateTime<Utc>,
        channel: SubmitChannel,
    ) -> Result<ReceiptWrite> {
        let mut conn = self.connect()?;
        let tx = begin_immediate(&mut conn)?;
        let Some(job) = load_job(&tx, &lease.job.id)? else {
            insert_job_event(
                &tx,
                &lease.job,
                "submit_receipt_after_lease_lost",
                &json!({ "channel": channel.as_str(), "token": token, "owner": lease.owner, "status": null, "stored": false }),
            )?;
            tx.commit()?;
            return Ok(ReceiptWrite::Logged);
        };

        if held_by(&job, &lease.owner, now) && job.status.can_transition(JobStatus::Processing) {
            write_receipt(
                &tx,
                &job,
                token,
                next_poll_at,
                Some(channel == SubmitChannel::Fallback),
            )?;
            let event_type = match channel {
                SubmitChannel::Primary => "submitted",
                SubmitChannel::Fallback => "submitted_via_fallback",
            };
            insert_job_event(
                &tx,
                &job,
                event_type,
                &json!({
                    "backend": job.backend,
                    "channel": channel.as_str(),
                    "token": token,
                    "pdf_hash": job.pdf_hash,
                    "snapshot_path": job.snapshot_path,
                }),
            )?;
            tx.commit()?;
            return Ok(ReceiptWrite::Accepted);
        }

        let store = job.token.is_none()
            && !lease_is_live(&job, now)
            && matches!(
                job.status,
                JobStatus::Submitted
                    | JobStatus::Failed
                    | JobStatus::FailedNeedsManual
                    | JobStatus::Timeout
            );
        if store {
            // A token always comes with started_at, which the review timeout counts from.
            tx.execute(
                "UPDATE jobs SET token = ?2, started_at = COALESCE(started_at, ?3) WHERE id = ?1",
                params![job.id, token, to_rfc3339(now)],
            )?;
            let mut write = RowWrite {
                fallback_used: Some(channel == SubmitChannel::Fallback),
                ..RowWrite::new(job.status, LeaseColumns::Clear)
            };
            if job.status == JobStatus::Submitted {
                let with_token = Job {
                    token: Some(token.to_string()),
                    ..job.clone()
                };
                write.submit_stage = Some(Some(SubmitStage::Uncertain));
                write.last_error = Some(Some(format!(
                    "submission receipt arrived after the worker lost its lease; {}",
                    with_token.reconcile_hint()
                )));
            }
            write_row(&tx, &job, write)?;
        }
        insert_job_event(
            &tx,
            &job,
            "submit_receipt_after_lease_lost",
            &json!({
                "channel": channel.as_str(),
                "token": token,
                "owner": lease.owner,
                "status": job.status.as_str(),
                "existing_token": job.token,
                "stored": store,
            }),
        )?;
        tx.commit()?;
        Ok(if store {
            ReceiptWrite::StoredForRecovery
        } else {
            ReceiptWrite::Logged
        })
    }

    /// Settle submit attempts whose owner is gone (crash, kill, stall):
    /// - QUEUED with an expired claim: nothing was sent, so the claim is released and
    ///   the job is claimable again with its cooldown unchanged.
    /// - SUBMITTED with no live owner and not yet UNCERTAIN: the provider may have
    ///   accepted it, so the job is marked UNCERTAIN with a diagnostic and is never
    ///   resubmitted automatically.
    pub fn recover_expired_leases(
        &self,
        project_id: &str,
        now: DateTime<Utc>,
    ) -> Result<LeaseRecovery> {
        let mut conn = self.connect()?;
        let tx = begin_immediate(&mut conn)?;
        let now_text = to_rfc3339(now);
        let mut report = LeaseRecovery::default();

        let stale_claims = query_jobs(
            &tx,
            r#"
            SELECT * FROM jobs
            WHERE project_id = ?1
              AND status = ?2
              AND lease_owner IS NOT NULL
              AND (lease_expires_at IS NULL OR lease_expires_at <= ?3)
            "#,
            params![project_id, JobStatus::Queued.as_str(), now_text],
        )?;
        for job in stale_claims {
            write_row(
                &tx,
                &job,
                RowWrite {
                    submit_stage: Some(None),
                    ..RowWrite::new(JobStatus::Queued, LeaseColumns::Clear)
                },
            )?;
            insert_job_event(
                &tx,
                &job,
                "submit_claim_expired",
                &json!({
                    "previous_owner": job.lease_owner,
                    "lease_expires_at": job.lease_expires_at.map(to_rfc3339),
                }),
            )?;
            report.released_claims += 1;
        }

        let orphaned = query_jobs(
            &tx,
            r#"
            SELECT * FROM jobs
            WHERE project_id = ?1
              AND status = ?2
              AND COALESCE(submit_stage, '') <> ?3
              AND (lease_owner IS NULL OR lease_expires_at IS NULL OR lease_expires_at <= ?4)
            "#,
            params![
                project_id,
                JobStatus::Submitted.as_str(),
                SubmitStage::Uncertain.as_str(),
                now_text
            ],
        )?;
        for job in orphaned {
            let (source, cause) = match job.lease_owner.as_deref() {
                Some(owner) => (
                    "lease_expired",
                    format!(
                        "worker {owner} lost its lease after dispatching to the provider (crash, kill, or stall)"
                    ),
                ),
                None => (
                    "legacy_submitted",
                    "the job was left SUBMITTED without a receipt by an earlier reviewloop version"
                        .to_string(),
                ),
            };
            let reason = format!(
                "submission outcome unknown: {cause}, so the provider may have accepted it; {}",
                job.reconcile_hint()
            );
            write_row(
                &tx,
                &job,
                RowWrite {
                    last_error: Some(Some(reason.clone())),
                    submit_stage: Some(Some(SubmitStage::Uncertain)),
                    ..RowWrite::new(JobStatus::Submitted, LeaseColumns::Clear)
                },
            )?;
            insert_job_event(
                &tx,
                &job,
                "submit_outcome_unknown",
                &json!({ "source": source, "previous_owner": job.lease_owner, "reason": reason }),
            )?;
            report.uncertain_submits += 1;
        }

        tx.commit()?;
        Ok(report)
    }

    /// Cancel a non-terminal job: FAILED with a "cancelled by user" reason, lease revoked,
    /// and a `cancelled` event, in one transaction — so a worker finishing concurrently
    /// either lands first (`AlreadyTerminal`) or loses its lease (its result is rejected).
    /// Cancelling never withdraws a request the provider may already hold.
    pub fn cancel_job(
        &self,
        job_id: &str,
        reason: Option<&str>,
        now: DateTime<Utc>,
    ) -> Result<CancelOutcome> {
        let mut conn = self.connect()?;
        let tx = begin_immediate(&mut conn)?;
        let job = require_job(&tx, job_id)?;
        if job.status.is_terminal() {
            return Ok(CancelOutcome::AlreadyTerminal(job.status));
        }

        let last_error = match reason {
            Some(reason) => format!("cancelled by user: {reason}"),
            None => "cancelled by user".to_string(),
        };
        let lease_was_active = lease_is_live(&job, now);
        write_row(
            &tx,
            &job,
            RowWrite {
                next_poll_at: Some(None),
                last_error: Some(Some(last_error)),
                submit_stage: Some(None),
                ..RowWrite::new(JobStatus::Failed, LeaseColumns::Clear)
            },
        )?;
        insert_job_event(
            &tx,
            &job,
            "cancelled",
            &json!({
                "reason": reason,
                "previous_status": job.status.as_str(),
                "previous_submit_stage": job.submit_stage.map(SubmitStage::as_str),
                "lease_was_active": lease_was_active,
            }),
        )?;
        tx.commit()?;
        Ok(CancelOutcome::Cancelled {
            previous_status: job.status,
            previous_stage: job.submit_stage,
            lease_was_active,
        })
    }

    pub fn upsert_review(
        &self,
        job_id: &str,
        token: &str,
        raw_json: &str,
        summary_md: &str,
    ) -> Result<()> {
        upsert_review_row(
            &self.connect()?,
            job_id,
            NewReview {
                token,
                raw_json,
                summary_md,
            },
        )
    }

    /// Read a job's stored review.
    pub fn get_review(&self, job_id: &str) -> Result<Option<ReviewRecord>> {
        let conn = self.connect()?;
        let row = conn
            .query_row(
                "SELECT token, raw_json, completed_at FROM reviews WHERE job_id = ?1",
                params![job_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()?;
        let Some((token, raw_json, completed_at)) = row else {
            return Ok(None);
        };
        Ok(Some(ReviewRecord {
            token,
            raw_json: serde_json::from_str(&raw_json)
                .with_context(|| format!("invalid review JSON stored for job {job_id}"))?,
            completed_at: parse_rfc3339(&completed_at)?,
        }))
    }

    /// When the job's review was stored, if it has one.
    pub fn review_completed_at(&self, job_id: &str) -> Result<Option<DateTime<Utc>>> {
        let conn = self.connect()?;
        let completed_at: Option<String> = conn
            .query_row(
                "SELECT completed_at FROM reviews WHERE job_id = ?1",
                params![job_id],
                |row| row.get(0),
            )
            .optional()?;
        completed_at.map(|value| parse_rfc3339(&value)).transpose()
    }

    /// A project's jobs, newest first, each with the time its review was
    /// stored. `active_only` keeps PENDING_APPROVAL, QUEUED, SUBMITTED and
    /// PROCESSING jobs.
    pub fn list_project_jobs(
        &self,
        project_id: &str,
        paper_id: Option<&str>,
        active_only: bool,
        limit: usize,
    ) -> Result<Vec<(Job, Option<DateTime<Utc>>)>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(
            r#"
            SELECT j.*, r.completed_at AS review_completed_at
            FROM jobs j
            LEFT JOIN reviews r ON r.job_id = j.id
            WHERE j.project_id = ?1
              AND (?2 IS NULL OR j.paper_id = ?2)
              AND (?3 = 0 OR j.status IN (?4, ?5, ?6, ?7))
            ORDER BY j.created_at DESC, j.id DESC
            LIMIT ?8
            "#,
        )?;
        let rows = stmt.query_map(
            params![
                project_id,
                paper_id,
                active_only,
                JobStatus::PendingApproval.as_str(),
                JobStatus::Queued.as_str(),
                JobStatus::Submitted.as_str(),
                JobStatus::Processing.as_str(),
                i64::try_from(limit).unwrap_or(i64::MAX),
            ],
            |row| {
                let review_completed_at = row
                    .get::<_, Option<String>>("review_completed_at")?
                    .map(|value| parse_rfc3339(&value))
                    .transpose()
                    .map_err(|e| conversion_error(e.to_string()))?;
                Ok((map_job_row(row)?, review_completed_at))
            },
        )?;
        collect_rows(rows)
    }

    pub fn add_event(
        &self,
        project_id: Option<&str>,
        job_id: Option<&str>,
        event_type: &str,
        payload: Value,
    ) -> Result<()> {
        insert_event(&self.connect()?, project_id, job_id, event_type, &payload)
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
        tx.execute(
            "DELETE FROM enqueue_requests WHERE job_id IN (SELECT id FROM jobs WHERE project_id = ?1 AND paper_id = ?2)",
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
                tx.execute(
                    &format!("DELETE FROM enqueue_requests WHERE job_id IN ({placeholders})"),
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
              AND (
                  last_error IS NULL
                  OR (last_error != 'cancelled by user' AND last_error NOT LIKE 'cancelled by user:%')
              )
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

    /// Fleet-wide: every active job across all projects.
    ///
    /// Used by `reviewloop-bar` to render a multi-project dashboard. Returned
    /// rows include the `project_id` column so callers can group by project.
    /// Bounded by `daemon.max_concurrency * num_projects` in practice; no
    /// LIMIT clause needed.
    pub fn list_active_jobs_all(&self) -> Result<Vec<Job>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(
            r#"
            SELECT *
            FROM jobs
            WHERE status IN (?1, ?2, ?3)
            ORDER BY project_id ASC, created_at ASC
            "#,
        )?;
        let rows = stmt.query_map(
            params![
                JobStatus::Queued.as_str(),
                JobStatus::Submitted.as_str(),
                JobStatus::Processing.as_str(),
            ],
            map_job_row,
        )?;
        collect_rows(rows)
    }

    /// Submissions `backend` has accepted or may have accepted, over every
    /// project in this database: provider credentials are machine-level, so
    /// usage is too. A job holding a token counts as accepted, wherever the
    /// token came from, and is split by status; a tokenless job left
    /// UNCERTAIN may have been accepted.
    pub fn provider_usage(&self, backend: &str) -> Result<ProviderUsage> {
        let conn = self.connect()?;
        let count = |row: &rusqlite::Row<'_>, index: usize| -> rusqlite::Result<u64> {
            Ok(row.get::<_, i64>(index)? as u64)
        };
        conn.query_row(
            r#"
            SELECT COALESCE(SUM(token IS NOT NULL AND status = ?2), 0),
                   COALESCE(SUM(token IS NOT NULL AND status IN (?3, ?4)), 0),
                   COALESCE(SUM(token IS NOT NULL AND status NOT IN (?2, ?3, ?4)), 0),
                   COALESCE(SUM(token IS NULL AND submit_stage = ?5), 0)
            FROM jobs
            WHERE backend = ?1
            "#,
            params![
                backend,
                JobStatus::Completed.as_str(),
                JobStatus::Processing.as_str(),
                JobStatus::Submitted.as_str(),
                SubmitStage::Uncertain.as_str(),
            ],
            |row| {
                Ok(ProviderUsage {
                    completed: count(row, 0)?,
                    in_progress: count(row, 1)?,
                    ended: count(row, 2)?,
                    uncertain: count(row, 3)?,
                })
            },
        )
        .map_err(Into::into)
    }

    /// Fleet-wide: recent failures across all projects, capped per project.
    ///
    /// Uses a window function so a single noisy project cannot starve
    /// failures from other projects out of the result set. Cancelled jobs
    /// (`last_error = 'cancelled by user'` or `last_error LIKE 'cancelled by user:%'`)
    /// are excluded.
    pub fn list_failed_jobs_all_per_project(&self, per_project_limit: usize) -> Result<Vec<Job>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(
            r#"
            SELECT id, paper_id, backend, pdf_path, pdf_hash, snapshot_path, status, token, email,
                   venue, review_options, git_tag, git_commit, attempt, started_at, next_poll_at,
                   last_error, fallback_used, created_at, updated_at,
                   project_id, version_no, round_no, version_source, version_key,
                   lease_owner, lease_expires_at, submit_stage
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY project_id ORDER BY updated_at DESC) AS rn
                FROM jobs
                WHERE status IN (?1, ?2, ?3)
                  AND (
                      last_error IS NULL
                      OR (last_error != 'cancelled by user' AND last_error NOT LIKE 'cancelled by user:%')
                  )
            )
            WHERE rn <= ?4
            ORDER BY project_id ASC, updated_at DESC
            "#,
        )?;
        let rows = stmt.query_map(
            params![
                JobStatus::Failed.as_str(),
                JobStatus::FailedNeedsManual.as_str(),
                JobStatus::Timeout.as_str(),
                per_project_limit as i64,
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

    /// Register or refresh the on-disk path of a project's `reviewloop.toml`
    /// (or a legacy global config carrying project settings).
    ///
    /// Called by `main::load_runtime` whenever a CLI invocation or daemon
    /// startup successfully loads a project context, so the registry stays
    /// up to date without explicit user action. The `(project_id, path)`
    /// pair lets `cmd_retry` / future fleet-wide commands resolve the right
    /// per-project config when called from a directory that has none.
    pub fn register_project_config(&self, project_id: &str, config_path: &Path) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            r#"
            INSERT INTO projects (project_id, config_path, last_seen_at)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(project_id) DO UPDATE SET
                config_path  = excluded.config_path,
                last_seen_at = excluded.last_seen_at
            "#,
            params![
                project_id,
                config_path.to_string_lossy(),
                Utc::now().to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    /// Look up the registered config path for a project_id, if any.
    pub fn resolve_project_config_path(&self, project_id: &str) -> Result<Option<PathBuf>> {
        let conn = self.connect()?;
        let path: Option<String> = conn
            .query_row(
                "SELECT config_path FROM projects WHERE project_id = ?1",
                params![project_id],
                |row| row.get(0),
            )
            .optional()?;
        Ok(path.map(PathBuf::from))
    }

    /// Every registered project, ordered by project_id.
    pub fn list_registered_projects(&self) -> Result<Vec<RegisteredProject>> {
        let conn = self.connect()?;
        let mut stmt = conn.prepare(
            "SELECT project_id, config_path, last_seen_at FROM projects ORDER BY project_id",
        )?;
        let rows = stmt.query_map([], |row| {
            let last_seen_at: String = row.get(2)?;
            Ok(RegisteredProject {
                project_id: row.get(0)?,
                config_path: PathBuf::from(row.get::<_, String>(1)?),
                last_seen_at: parse_rfc3339(&last_seen_at)
                    .map_err(|e| conversion_error(e.to_string()))?,
            })
        })?;
        collect_rows(rows)
    }

    /// Remove a stale registry entry. Called when a registered path no
    /// longer exists on disk so the next CLI invocation in that project
    /// repo can re-register cleanly.
    pub fn forget_project_registration(&self, project_id: &str) -> Result<()> {
        let conn = self.connect()?;
        conn.execute(
            "DELETE FROM projects WHERE project_id = ?1",
            params![project_id],
        )?;
        Ok(())
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

/// Begin a transaction that takes the write lock up front. A deferred read-then-write
/// transaction in WAL mode fails with SQLITE_BUSY — without a busy-handler retry — when
/// another connection commits in between, so every read-validate-write uses this.
fn begin_immediate(conn: &mut Connection) -> Result<Transaction<'_>> {
    Ok(conn.transaction_with_behavior(TransactionBehavior::Immediate)?)
}

fn schema_version(conn: &Connection) -> Result<u32> {
    let version: i64 = conn
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .context("reading schema version")?;
    Ok(u32::try_from(version).unwrap_or(0))
}

fn require_job(conn: &Connection, job_id: &str) -> Result<Job> {
    load_job(conn, job_id)?.ok_or_else(|| anyhow!("job not found: {job_id}"))
}

fn query_jobs(conn: &Connection, sql: &str, params: impl rusqlite::Params) -> Result<Vec<Job>> {
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt.query_map(params, map_job_row)?;
    collect_rows(rows)
}

fn lease_is_live(job: &Job, now: DateTime<Utc>) -> bool {
    job.lease_owner.is_some() && job.lease_expires_at.is_some_and(|at| at > now)
}

fn held_by(job: &Job, owner: &str, now: DateTime<Utc>) -> bool {
    job.lease_owner.as_deref() == Some(owner) && lease_is_live(job, now)
}

enum LeaseColumns<'a> {
    Keep,
    Clear,
    Set {
        owner: &'a str,
        expires_at: DateTime<Utc>,
    },
}

/// A change to a job's execution state. `None` fields keep the current value.
struct RowWrite<'a> {
    status: JobStatus,
    attempt: Option<u32>,
    next_poll_at: Option<Option<DateTime<Utc>>>,
    last_error: Option<Option<String>>,
    lease: LeaseColumns<'a>,
    submit_stage: Option<Option<SubmitStage>>,
    fallback_used: Option<bool>,
}

impl<'a> RowWrite<'a> {
    fn new(status: JobStatus, lease: LeaseColumns<'a>) -> Self {
        Self {
            status,
            attempt: None,
            next_poll_at: None,
            last_error: None,
            lease,
            submit_stage: None,
            fallback_used: None,
        }
    }
}

/// The single writer of a job's execution-state columns — status, attempt, schedule,
/// last_error, lease, submit stage and fallback flag — so every state change keeps them
/// consistent.
fn write_row(conn: &Connection, current: &Job, write: RowWrite<'_>) -> Result<()> {
    let (lease_owner, lease_expires_at) = match write.lease {
        LeaseColumns::Keep => (current.lease_owner.clone(), current.lease_expires_at),
        LeaseColumns::Clear => (None, None),
        LeaseColumns::Set { owner, expires_at } => (Some(owner.to_string()), Some(expires_at)),
    };
    conn.execute(
        r#"
        UPDATE jobs
        SET status = ?2,
            attempt = ?3,
            next_poll_at = ?4,
            last_error = ?5,
            lease_owner = ?6,
            lease_expires_at = ?7,
            submit_stage = ?8,
            fallback_used = ?9,
            updated_at = ?10
        WHERE id = ?1
        "#,
        params![
            current.id,
            write.status.as_str(),
            write.attempt.unwrap_or(current.attempt) as i64,
            write
                .next_poll_at
                .unwrap_or(current.next_poll_at)
                .map(to_rfc3339),
            write
                .last_error
                .unwrap_or_else(|| current.last_error.clone()),
            lease_owner,
            lease_expires_at.map(to_rfc3339),
            write
                .submit_stage
                .unwrap_or(current.submit_stage)
                .map(SubmitStage::as_str),
            write.fallback_used.unwrap_or(current.fallback_used),
            to_rfc3339(Utc::now()),
        ],
    )?;
    Ok(())
}

/// Move a job to PROCESSING with its receipt token, releasing any lease.
/// Move `current` to PROCESSING with `token`. `fallback_used` records the route that
/// produced the receipt when it is known; once a job holds a token the flag no longer
/// guards a rerun (the job is polled, never resubmitted), so it names that route.
fn write_receipt(
    conn: &Connection,
    current: &Job,
    token: &str,
    next_poll_at: DateTime<Utc>,
    fallback_used: Option<bool>,
) -> Result<()> {
    write_row(
        conn,
        current,
        RowWrite {
            attempt: Some(0),
            next_poll_at: Some(Some(next_poll_at)),
            last_error: Some(None),
            submit_stage: Some(None),
            fallback_used,
            ..RowWrite::new(JobStatus::Processing, LeaseColumns::Clear)
        },
    )?;
    conn.execute(
        "UPDATE jobs SET token = ?2, started_at = COALESCE(started_at, ?3) WHERE id = ?1",
        params![current.id, token, to_rfc3339(Utc::now())],
    )?;
    Ok(())
}

fn insert_job_event(conn: &Connection, job: &Job, event_type: &str, payload: &Value) -> Result<()> {
    insert_event(
        conn,
        Some(&job.project_id),
        Some(&job.id),
        event_type,
        payload,
    )
}

fn upsert_review_row(conn: &Connection, job_id: &str, review: NewReview<'_>) -> Result<()> {
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
        params![
            job_id,
            review.token,
            review.raw_json,
            review.summary_md,
            to_rfc3339(Utc::now())
        ],
    )?;
    Ok(())
}

fn enable_wal_mode(conn: &Connection) -> Result<()> {
    // Enable WAL journal mode for file-based databases. WAL allows concurrent
    // readers + one writer without blocking each other, so a write transaction
    // in one connection (e.g. update_job_state) does not starve another
    // connection's write (e.g. emit_failover_event). For in-memory databases
    // this pragma is silently ignored (mode stays "memory"), which is fine
    // since in-memory DBs are single-process and don't have the
    // concurrent-connection issue.
    let _ = conn.execute_batch("PRAGMA journal_mode = WAL;");

    // Verify the journal mode; in-memory DBs return "memory", which is expected.
    let mode: String = conn
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .unwrap_or_else(|_| "unknown".to_string());
    if !(mode.eq_ignore_ascii_case("wal") || mode.eq_ignore_ascii_case("memory")) {
        tracing::warn!(
            actual_mode = %mode,
            "expected WAL journal mode but got '{}'; concurrency guarantees may be degraded",
            mode
        );
    }

    Ok(())
}

fn create_tables_if_missing(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            project_id TEXT NOT NULL DEFAULT '',
            paper_id TEXT NOT NULL,
            backend TEXT NOT NULL,
            pdf_path TEXT NOT NULL,
            pdf_hash TEXT NOT NULL,
            snapshot_path TEXT,
            status TEXT NOT NULL,
            token TEXT,
            email TEXT NOT NULL,
            venue TEXT,
            review_options TEXT,
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
            lease_owner TEXT,
            lease_expires_at TEXT,
            submit_stage TEXT,
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

        CREATE TABLE IF NOT EXISTS projects (
            project_id   TEXT PRIMARY KEY,
            config_path  TEXT NOT NULL,
            last_seen_at TEXT NOT NULL
        );

        -- Idempotency keys: each binds to the job its first request resolved
        -- to. Several keys may share one job (a covered request records its
        -- key against the covering job).
        CREATE TABLE IF NOT EXISTS enqueue_requests (
            project_id    TEXT NOT NULL,
            request_key   TEXT NOT NULL,
            identity_json TEXT NOT NULL,
            job_id        TEXT NOT NULL,
            created_at    TEXT NOT NULL,
            PRIMARY KEY (project_id, request_key)
        );
        "#,
    )?;
    Ok(())
}

fn migrate_columns(conn: &Connection) -> Result<()> {
    // Backfill columns that were added in later versions. MUST run before
    // CREATE INDEX, since some indexes reference columns (project_id,
    // version_key) that an older schema lacks.
    ensure_column_exists(conn, "jobs", "project_id", "TEXT NOT NULL DEFAULT ''")?;
    ensure_column_exists(conn, "jobs", "started_at", "TEXT")?;
    ensure_column_exists(conn, "jobs", "version_no", "INTEGER NOT NULL DEFAULT 1")?;
    ensure_column_exists(conn, "jobs", "round_no", "INTEGER NOT NULL DEFAULT 1")?;
    ensure_column_exists(
        conn,
        "jobs",
        "version_source",
        "TEXT NOT NULL DEFAULT 'pdf_hash'",
    )?;
    ensure_column_exists(conn, "jobs", "version_key", "TEXT NOT NULL DEFAULT ''")?;
    ensure_column_exists(conn, "events", "project_id", "TEXT NOT NULL DEFAULT ''")?;
    ensure_column_exists(conn, "jobs", "lease_owner", "TEXT")?;
    ensure_column_exists(conn, "jobs", "lease_expires_at", "TEXT")?;
    ensure_column_exists(conn, "jobs", "submit_stage", "TEXT")?;
    ensure_column_exists(conn, "jobs", "snapshot_path", "TEXT")?;
    ensure_column_exists(conn, "jobs", "review_options", "TEXT")?;

    if column_exists(conn, "jobs", "version_no")? {
        conn.execute(
            "UPDATE jobs SET version_no = 1 WHERE version_no IS NULL OR version_no = 0",
            [],
        )?;
    }
    if column_exists(conn, "jobs", "round_no")? {
        conn.execute(
            "UPDATE jobs SET round_no = 1 WHERE round_no IS NULL OR round_no = 0",
            [],
        )?;
    }
    if column_exists(conn, "jobs", "version_source")? && column_exists(conn, "jobs", "git_commit")?
    {
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
    }
    if column_exists(conn, "jobs", "version_key")?
        && column_exists(conn, "jobs", "git_commit")?
        && column_exists(conn, "jobs", "pdf_hash")?
    {
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
    }
    if column_exists(conn, "events", "project_id")?
        && column_exists(conn, "events", "job_id")?
        && column_exists(conn, "jobs", "project_id")?
    {
        conn.execute(
            r#"
            UPDATE events
            SET project_id = COALESCE((SELECT jobs.project_id FROM jobs WHERE jobs.id = events.job_id), '')
            WHERE COALESCE(project_id, '') = ''
            "#,
            [],
        )?;
    }

    Ok(())
}

fn create_indexes(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE INDEX IF NOT EXISTS idx_jobs_project_status_next_poll ON jobs(project_id, status, next_poll_at);
        CREATE INDEX IF NOT EXISTS idx_jobs_project_backend_hash ON jobs(project_id, backend, pdf_hash);
        CREATE INDEX IF NOT EXISTS idx_jobs_project_paper_backend ON jobs(project_id, paper_id, backend);
        CREATE INDEX IF NOT EXISTS idx_jobs_project_dedupe ON jobs(project_id, paper_id, backend, pdf_hash, version_key, status);
        CREATE INDEX IF NOT EXISTS idx_events_project_created_at ON events(project_id, created_at);
        CREATE INDEX IF NOT EXISTS idx_enqueue_requests_job ON enqueue_requests(job_id);
        "#,
    )?;
    Ok(())
}

/// Statuses of a job that answers its review request: pending, in flight, or
/// done. Failed, timed-out and cancelled jobs answer nothing, so they neither
/// cover a new request nor hold a review round.
const COVERING_STATUSES: [JobStatus; 5] = [
    JobStatus::PendingApproval,
    JobStatus::Queued,
    JobStatus::Submitted,
    JobStatus::Processing,
    JobStatus::Completed,
];

fn enqueue_in_tx(conn: &Connection, request: &EnqueueRequest) -> Result<EnqueueOutcome> {
    let new_job = &request.job;
    let project_id = new_job.project_id.as_str();
    let identity = new_job.review_identity();

    if let Some(key) = request.request_key.as_deref() {
        let bound: Option<(String, String)> = conn
            .query_row(
                "SELECT identity_json, job_id FROM enqueue_requests WHERE project_id = ?1 AND request_key = ?2",
                params![project_id, key],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if let Some((identity_json, job_id)) = bound {
            let recorded: ReviewIdentity = serde_json::from_str(&identity_json)
                .with_context(|| format!("corrupt identity recorded for request key {key:?}"))?;
            let mismatches = recorded.mismatches(&identity);
            if !mismatches.is_empty() {
                return Err(EnqueueConflict {
                    project_id: project_id.to_string(),
                    request_key: key.to_string(),
                    existing_job_id: job_id,
                    mismatches,
                }
                .into());
            }
            let job = load_job(conn, &job_id)?.ok_or_else(|| {
                anyhow!("request key {key:?} is bound to job {job_id}, which no longer exists")
            })?;
            return Ok(EnqueueOutcome::Existing {
                job,
                reason: ExistingReason::RequestReplay,
            });
        }
    }

    if request.mode == EnqueueMode::Deduplicate
        && let Some(job) = find_covering_job(conn, project_id, &identity)?
    {
        bind_request_key(conn, request, &identity, &job.id)?;
        insert_duplicate_skipped(
            conn,
            project_id,
            &identity,
            &job,
            request.request_key.as_deref(),
            &request.source,
        )?;
        return Ok(EnqueueOutcome::Existing {
            job,
            reason: ExistingReason::Covered,
        });
    }

    let job = insert_job(conn, new_job, &identity)?;
    bind_request_key(conn, request, &identity, &job.id)?;
    insert_event(
        conn,
        Some(project_id),
        Some(&job.id),
        "job_enqueued",
        &json!({
            "source": request.source,
            "enqueue_mode": request.mode.as_str(),
            "request_key": request.request_key,
            "status": job.status.as_str(),
            "paper_id": job.paper_id,
            "backend": job.backend,
            "pdf_hash": job.pdf_hash,
            "venue": job.venue,
            "review_options": job.review_options,
            "version_no": job.version_no,
            "round_no": job.round_no,
            "version_source": job.version_source,
            "version_key": job.version_key,
        }),
    )?;
    Ok(EnqueueOutcome::Created(job))
}

fn insert_duplicate_skipped(
    conn: &Connection,
    project_id: &str,
    identity: &ReviewIdentity,
    existing: &Job,
    request_key: Option<&str>,
    source: &str,
) -> Result<()> {
    insert_event(
        conn,
        Some(project_id),
        None,
        "duplicate_skipped",
        &json!({
            "project_id": project_id,
            "paper_id": identity.paper_id,
            "backend": identity.backend,
            "pdf_hash": identity.pdf_hash,
            "venue": identity.venue,
            "review_options": identity.review_options,
            "version_no": existing.version_no,
            "round_no": existing.round_no,
            "version_source": identity.version_source.as_str(),
            "version_key": identity.version_key,
            "existing_job_id": existing.id,
            "existing_job_status": existing.status.as_str(),
            "request_key": request_key,
            "source": source,
        }),
    )
}

fn bind_request_key(
    conn: &Connection,
    request: &EnqueueRequest,
    identity: &ReviewIdentity,
    job_id: &str,
) -> Result<()> {
    let Some(key) = request.request_key.as_deref() else {
        return Ok(());
    };
    conn.execute(
        r#"
        INSERT INTO enqueue_requests(project_id, request_key, identity_json, job_id, created_at)
        VALUES (?1, ?2, ?3, ?4, ?5)
        "#,
        params![
            request.job.project_id,
            key,
            serde_json::to_string(identity)?,
            job_id,
            to_rfc3339(Utc::now()),
        ],
    )?;
    Ok(())
}

fn find_covering_job(
    conn: &Connection,
    project_id: &str,
    identity: &ReviewIdentity,
) -> Result<Option<Job>> {
    // Rows written before venues were normalized may hold blanks or padding.
    // Rows written before review options existed hold NULL, which matches the
    // empty options of a backend that has none.
    let [s1, s2, s3, s4, s5] = COVERING_STATUSES.map(JobStatus::as_str);
    conn.query_row(
        r#"
        SELECT *
        FROM jobs
        WHERE project_id = ?1
          AND paper_id = ?2
          AND backend = ?3
          AND pdf_hash = ?4
          AND version_key = ?5
          AND COALESCE(TRIM(venue), '') = ?6
          AND COALESCE(review_options, '') = ?12
          AND status IN (?7, ?8, ?9, ?10, ?11)
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        "#,
        params![
            project_id,
            identity.paper_id,
            identity.backend,
            identity.pdf_hash,
            identity.version_key,
            identity.venue.as_deref().unwrap_or(""),
            s1,
            s2,
            s3,
            s4,
            s5,
            identity.review_options.canonical().unwrap_or_default(),
        ],
        map_job_row,
    )
    .optional()
    .map_err(Into::into)
}

fn insert_job(conn: &Connection, new_job: &NewJob, identity: &ReviewIdentity) -> Result<Job> {
    let now = Utc::now();
    let id = Uuid::new_v4().to_string();
    let (version_no, round_no) = determine_versioning(
        conn,
        &new_job.project_id,
        &new_job.paper_id,
        &identity.version_key,
    )?;

    conn.execute(
        r#"
        INSERT INTO jobs (
            id, project_id, paper_id, backend, pdf_path, pdf_hash, snapshot_path, status, token,
            email, venue, review_options, git_tag, git_commit, version_no, round_no,
            version_source, version_key, attempt, started_at, next_poll_at, last_error,
            fallback_used, created_at, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL, ?9, ?10, ?19, ?11, ?12, ?13, ?14, ?15, ?16, 0, NULL, ?17, NULL, 0, ?18, ?18)
        "#,
        params![
            id,
            new_job.project_id,
            new_job.paper_id,
            new_job.backend,
            new_job.pdf.pdf_path(),
            new_job.pdf.pdf_hash(),
            new_job.pdf.snapshot_path(),
            new_job.status.as_str(),
            new_job.email,
            identity.venue,
            new_job.git_tag,
            new_job.git_commit,
            version_no as i64,
            round_no as i64,
            identity.version_source.as_str(),
            identity.version_key,
            new_job.next_poll_at.map(to_rfc3339),
            to_rfc3339(now),
            identity.review_options.canonical(),
        ],
    )?;

    load_job(conn, &id)?.ok_or_else(|| anyhow!("failed to load inserted job: {id}"))
}

/// Read through `conn` so rows written earlier in the same transaction are visible.
fn load_job(conn: &Connection, job_id: &str) -> Result<Option<Job>> {
    conn.query_row(
        "SELECT * FROM jobs WHERE id = ?1",
        params![job_id],
        map_job_row,
    )
    .optional()
    .map_err(Into::into)
}

fn insert_event(
    conn: &Connection,
    project_id: Option<&str>,
    job_id: Option<&str>,
    event_type: &str,
    payload: &Value,
) -> Result<()> {
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

/// Allocate `(version_no, round_no)` for a new job of `paper_id`.
///
/// The version stays the same while the version key matches the paper's most
/// recent job and otherwise takes the next unused number. The round is one past
/// the highest round still held by a pending, in-flight or completed job of
/// that version, so every live review of a version has its own round, while a
/// failed attempt gives its round back to the retry. Callers must hold the
/// write lock (see [`Db::enqueue`]) so that concurrent jobs cannot be handed
/// the same numbers.
fn determine_versioning(
    conn: &Connection,
    project_id: &str,
    paper_id: &str,
    version_key: &str,
) -> Result<(u32, u32)> {
    let latest: Option<(u32, String)> = conn
        .query_row(
            r#"
            SELECT version_no, version_key
            FROM jobs
            WHERE project_id = ?1 AND paper_id = ?2
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            "#,
            params![project_id, paper_id],
            |row| Ok((row.get::<_, i64>(0)? as u32, row.get::<_, String>(1)?)),
        )
        .optional()?;

    let version_no = match latest {
        Some((latest_version_no, latest_version_key)) if latest_version_key == version_key => {
            latest_version_no
        }
        Some(_) => conn.query_row(
            "SELECT COALESCE(MAX(version_no), 0) + 1 FROM jobs WHERE project_id = ?1 AND paper_id = ?2",
            params![project_id, paper_id],
            |row| Ok(row.get::<_, i64>(0)? as u32),
        )?,
        None => 1,
    };

    let [s1, s2, s3, s4, s5] = COVERING_STATUSES.map(JobStatus::as_str);
    let round_no = conn.query_row(
        r#"
        SELECT COALESCE(MAX(round_no), 0) + 1
        FROM jobs
        WHERE project_id = ?1
          AND paper_id = ?2
          AND version_no = ?3
          AND status IN (?4, ?5, ?6, ?7, ?8)
        "#,
        params![project_id, paper_id, version_no as i64, s1, s2, s3, s4, s5],
        |row| Ok(row.get::<_, i64>(0)? as u32),
    )?;

    Ok((version_no, round_no))
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
    let lease_expires_at: Option<String> = row.get("lease_expires_at")?;
    let submit_stage: Option<String> = row.get("submit_stage")?;
    let review_options: Option<String> = row.get("review_options")?;
    let created_at: String = row.get("created_at")?;
    let updated_at: String = row.get("updated_at")?;

    let status = JobStatus::from_db(&status)
        .ok_or_else(|| conversion_error(format!("invalid status: {status}")))?;
    let submit_stage = submit_stage
        .map(|value| {
            SubmitStage::from_db(&value)
                .ok_or_else(|| conversion_error(format!("invalid submit_stage: {value}")))
        })
        .transpose()?;
    let review_options = ReviewOptions::from_canonical(review_options.as_deref())
        .map_err(|e| conversion_error(format!("invalid review_options: {e}")))?;
    let lease_expires_at = lease_expires_at
        .map(|v| parse_rfc3339(&v))
        .transpose()
        .map_err(|e| conversion_error(e.to_string()))?;

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
        snapshot_path: row.get("snapshot_path")?,
        status,
        token: row.get("token")?,
        email: row.get("email")?,
        venue: row.get("venue")?,
        review_options,
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
        lease_owner: row.get("lease_owner")?,
        lease_expires_at,
        submit_stage,
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

fn column_exists(conn: &Connection, table: &str, column: &str) -> Result<bool> {
    let pragma = format!(r#"PRAGMA table_info("{table}")"#);
    let mut stmt = conn.prepare(&pragma)?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        if row.get::<_, String>(1)? == column {
            return Ok(true);
        }
    }
    Ok(false)
}

fn ensure_column_exists(
    conn: &Connection,
    table: &str,
    column: &str,
    column_def: &str,
) -> Result<()> {
    if column_exists(conn, table, column)? {
        return Ok(());
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
    use crate::model::{JobPdf, JobStatus, NewJob};
    use tempfile::tempdir;

    fn make_queued_job(project_id: &str, paper_id: &str) -> NewJob {
        NewJob {
            project_id: project_id.to_string(),
            paper_id: paper_id.to_string(),
            backend: "stanford".to_string(),
            pdf: JobPdf::Unpinned {
                pdf_path: "paper.pdf".to_string(),
                pdf_hash: "abc123".to_string(),
            },
            status: JobStatus::Queued,
            email: "test@example.com".to_string(),
            venue: None,
            review_options: Default::default(),
            git_tag: None,
            git_commit: None,
            next_poll_at: None,
        }
    }

    /// Verify that WAL journal mode is enabled after ensure_schema.
    ///
    /// With WAL enabled, concurrent writes from separate connections (e.g.
    /// update_job_state holding a transaction while emit_failover_event opens
    /// a fresh connection) are retried rather than failing immediately, fixing
    /// the "failover events silently dropped under load" bug (N2).
    #[test]
    fn wal_mode_enabled_after_ensure_schema() {
        let tmp = tempdir().unwrap();
        let db = Db::new(tmp.path());
        db.ensure_schema().expect("ensure_schema must succeed");

        let conn = db.connect().expect("connect after ensure_schema");
        let mode: String = conn
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .expect("PRAGMA journal_mode must return a row");
        assert_eq!(
            mode, "wal",
            "expected WAL journal mode after ensure_schema; got: {mode}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn db_file_is_0o600_after_creation() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempdir().unwrap();
        let db = Db::new(tmp.path());
        db.ensure_schema().expect("ensure_schema must succeed");

        let mode = std::fs::metadata(&db.path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "database file must be 0o600 after creation");
    }

    #[test]
    fn ensure_schema_sets_user_version() {
        let db = Db::new_in_memory("schema_version_test").unwrap();
        db.ensure_schema().expect("ensure_schema must succeed");

        let conn = db.connect().expect("connect after ensure_schema");
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .expect("PRAGMA user_version must return a row");
        assert_eq!(version, SCHEMA_VERSION as i64);
    }

    /// A v1 database (everything but the request-key table) gains the table
    /// and index on upgrade, keeps its jobs, and enqueues against them.
    /// A v2 database (no `snapshot_path`) upgrades in place: existing rows
    /// read back unpinned, and new rows can be pinned.
    #[test]
    fn ensure_schema_adds_snapshot_path_to_v2_database() {
        let tmp = tempdir().unwrap();
        let db_path = tmp.path().join("v2.db");
        {
            let conn = rusqlite::Connection::open(&db_path).unwrap();
            create_tables_if_missing(&conn).unwrap();
            conn.execute_batch(
                r#"
                ALTER TABLE jobs DROP COLUMN snapshot_path;
                INSERT INTO jobs (id, project_id, paper_id, backend, pdf_path, pdf_hash, status, email, created_at, updated_at)
                VALUES ('legacy', 'proj', 'paper', 'stanford', 'legacy/a.pdf', 'hash-a', 'QUEUED', 'a@example.com', '2025-02-01T00:00:00Z', '2025-02-01T00:00:00Z');
                PRAGMA user_version = 2;
                "#,
            )
            .unwrap();
            assert!(!column_exists(&conn, "jobs", "snapshot_path").unwrap());
        }

        let db = Db::new_file(db_path);
        db.ensure_schema().expect("v2 -> v3 migration");

        let legacy = db.get_job("legacy").unwrap().expect("legacy row");
        assert_eq!(legacy.snapshot_path, None);
        assert_eq!(legacy.pdf_hash, "hash-a");

        db.set_job_snapshot("legacy", Path::new("snapshots/hash-a/a.pdf"))
            .unwrap();
        let backfilled = db.get_job("legacy").unwrap().expect("legacy row");
        assert_eq!(
            backfilled.snapshot_path.as_deref(),
            Some("snapshots/hash-a/a.pdf")
        );

        let pinned = db
            .create_job(&NewJob {
                pdf: JobPdf::Pinned(crate::submission_input::PreparedInput {
                    source_path: "paper.pdf".into(),
                    snapshot_path: "snapshots/hash-b/paper.pdf".into(),
                    sha256: "hash-b".to_string(),
                }),
                ..make_queued_job("proj", "paper")
            })
            .unwrap();
        assert_eq!(pinned.pdf_path, "paper.pdf");
        assert_eq!(pinned.pdf_hash, "hash-b");
        assert_eq!(
            pinned.snapshot_path.as_deref(),
            Some("snapshots/hash-b/paper.pdf")
        );
        assert_eq!(
            db.list_job_pdf_hashes().unwrap(),
            HashSet::from(["hash-a".to_string(), "hash-b".to_string()])
        );
    }

    #[test]
    fn ensure_schema_upgrades_v1_database_with_enqueue_requests() {
        let tmp = tempdir().unwrap();
        let db = Db::new(tmp.path());
        db.ensure_schema().unwrap();
        let legacy = db.create_job(&make_queued_job("proj", "paper")).unwrap();
        {
            let conn = db.connect().unwrap();
            conn.execute_batch("DROP TABLE enqueue_requests;").unwrap();
            conn.pragma_update(None, "user_version", 1).unwrap();
        }

        db.ensure_schema().unwrap();

        let conn = db.connect().unwrap();
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION as i64);
        let index: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'idx_enqueue_requests_job'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(index, 1);
        let outcome = db
            .enqueue(&EnqueueRequest {
                job: make_queued_job("proj", "paper"),
                request_key: Some("after-upgrade".to_string()),
                mode: EnqueueMode::Deduplicate,
                source: "test".to_string(),
            })
            .unwrap();
        match outcome {
            EnqueueOutcome::Existing { job, reason } => {
                assert_eq!(job.id, legacy.id);
                assert_eq!(reason, ExistingReason::Covered);
            }
            other => panic!("legacy job should cover the request, got {other:?}"),
        }
    }

    #[test]
    fn ensure_schema_skips_migrations_when_already_at_current_version() {
        let tmp = tempdir().unwrap();
        let db_path = tmp.path().join("current-version.db");

        {
            let conn = rusqlite::Connection::open(&db_path).unwrap();
            conn.execute_batch(
                r#"
                CREATE TABLE jobs (
                    id TEXT PRIMARY KEY,
                    paper_id TEXT NOT NULL
                );
                "#,
            )
            .unwrap();
            conn.pragma_update(None, "user_version", SCHEMA_VERSION as i64)
                .unwrap();
        }

        let db = Db::new_file(db_path);
        db.ensure_schema()
            .expect("ensure_schema should be a no-op at current schema version");

        let conn = db.connect().unwrap();
        assert!(
            !column_exists(&conn, "jobs", "project_id").unwrap(),
            "current-version DBs should skip legacy column migrations"
        );
        let idx_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'idx_jobs_project_status_next_poll'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(idx_count, 0, "current-version DBs should skip index work");
    }

    /// Regression: upgrading from a pre-Phase-0 schema (jobs table without
    /// the project_id column) used to fail in ensure_schema because CREATE
    /// INDEX on (project_id, ...) ran BEFORE ensure_column_exists added the
    /// missing column. Reproduces the production breakage by hand-crafting
    /// an old-shape jobs table, then runs ensure_schema and asserts indexes
    /// got created and old rows are still readable with existing values intact.
    #[test]
    fn ensure_schema_migrates_pre_project_id_table() {
        #[derive(Debug, PartialEq)]
        struct LegacyJobSnapshot {
            id: String,
            paper_id: String,
            backend: String,
            pdf_path: String,
            pdf_hash: String,
            status: String,
            token: Option<String>,
            email: String,
            venue: Option<String>,
            git_tag: Option<String>,
            git_commit: Option<String>,
            attempt: i64,
            next_poll_at: Option<String>,
            last_error: Option<String>,
            fallback_used: i64,
            created_at: String,
            updated_at: String,
        }

        #[derive(Debug, PartialEq)]
        struct MigratedJobColumns {
            id: String,
            project_id: String,
            started_at: Option<String>,
            version_no: i64,
            round_no: i64,
            version_source: String,
            version_key: String,
        }

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
                INSERT INTO jobs (
                    id, paper_id, backend, pdf_path, pdf_hash, status, token, email, venue,
                    git_tag, git_commit, attempt, next_poll_at, last_error, fallback_used,
                    created_at, updated_at
                ) VALUES
                    ('legacy-job-1', 'paper-a', 'stanford', 'legacy/a.pdf', 'hash-a', 'COMPLETED', 'tok-a', 'a@example.com', 'ICLR', 'v1.0.0', 'commit-a', 2, '2025-01-01T00:05:00Z', 'err-a', 1, '2025-01-01T00:00:00Z', '2025-01-01T00:10:00Z'),
                    ('legacy-job-2', 'paper-b', 'openreview', 'legacy/b.pdf', 'hash-b', 'QUEUED', NULL, 'b@example.com', 'NeurIPS', NULL, NULL, 0, NULL, NULL, 0, '2025-01-02T00:00:00Z', '2025-01-02T00:10:00Z'),
                    ('legacy-job-3', 'paper-c', 'stanford', 'legacy/c.pdf', 'hash-c', 'PROCESSING', 'tok-c', 'c@example.com', NULL, 'v3.0.0', '', 5, '2025-01-03T00:05:00Z', 'retrying', 0, '2025-01-03T00:00:00Z', '2025-01-03T00:10:00Z');
                "#,
            )
            .unwrap();
        }

        // Now run ensure_schema -- this used to fail with "no such column:
        // project_id" because CREATE INDEX ran before ensure_column_exists.
        let db = Db::new_file(db_path.clone());
        db.ensure_schema()
            .expect("ensure_schema must succeed on legacy db");

        let conn = db.connect().unwrap();
        let mut stmt = conn
            .prepare(
                r#"
                SELECT id, paper_id, backend, pdf_path, pdf_hash, status, token, email, venue,
                       git_tag, git_commit, attempt, next_poll_at, last_error, fallback_used,
                       created_at, updated_at
                FROM jobs
                ORDER BY id
                "#,
            )
            .unwrap();
        let rows: Vec<LegacyJobSnapshot> = stmt
            .query_map([], |row| {
                Ok(LegacyJobSnapshot {
                    id: row.get(0)?,
                    paper_id: row.get(1)?,
                    backend: row.get(2)?,
                    pdf_path: row.get(3)?,
                    pdf_hash: row.get(4)?,
                    status: row.get(5)?,
                    token: row.get(6)?,
                    email: row.get(7)?,
                    venue: row.get(8)?,
                    git_tag: row.get(9)?,
                    git_commit: row.get(10)?,
                    attempt: row.get(11)?,
                    next_poll_at: row.get(12)?,
                    last_error: row.get(13)?,
                    fallback_used: row.get(14)?,
                    created_at: row.get(15)?,
                    updated_at: row.get(16)?,
                })
            })
            .unwrap()
            .map(|row| row.unwrap())
            .collect();
        assert_eq!(
            rows,
            vec![
                LegacyJobSnapshot {
                    id: "legacy-job-1".to_string(),
                    paper_id: "paper-a".to_string(),
                    backend: "stanford".to_string(),
                    pdf_path: "legacy/a.pdf".to_string(),
                    pdf_hash: "hash-a".to_string(),
                    status: "COMPLETED".to_string(),
                    token: Some("tok-a".to_string()),
                    email: "a@example.com".to_string(),
                    venue: Some("ICLR".to_string()),
                    git_tag: Some("v1.0.0".to_string()),
                    git_commit: Some("commit-a".to_string()),
                    attempt: 2,
                    next_poll_at: Some("2025-01-01T00:05:00Z".to_string()),
                    last_error: Some("err-a".to_string()),
                    fallback_used: 1,
                    created_at: "2025-01-01T00:00:00Z".to_string(),
                    updated_at: "2025-01-01T00:10:00Z".to_string(),
                },
                LegacyJobSnapshot {
                    id: "legacy-job-2".to_string(),
                    paper_id: "paper-b".to_string(),
                    backend: "openreview".to_string(),
                    pdf_path: "legacy/b.pdf".to_string(),
                    pdf_hash: "hash-b".to_string(),
                    status: "QUEUED".to_string(),
                    token: None,
                    email: "b@example.com".to_string(),
                    venue: Some("NeurIPS".to_string()),
                    git_tag: None,
                    git_commit: None,
                    attempt: 0,
                    next_poll_at: None,
                    last_error: None,
                    fallback_used: 0,
                    created_at: "2025-01-02T00:00:00Z".to_string(),
                    updated_at: "2025-01-02T00:10:00Z".to_string(),
                },
                LegacyJobSnapshot {
                    id: "legacy-job-3".to_string(),
                    paper_id: "paper-c".to_string(),
                    backend: "stanford".to_string(),
                    pdf_path: "legacy/c.pdf".to_string(),
                    pdf_hash: "hash-c".to_string(),
                    status: "PROCESSING".to_string(),
                    token: Some("tok-c".to_string()),
                    email: "c@example.com".to_string(),
                    venue: None,
                    git_tag: Some("v3.0.0".to_string()),
                    git_commit: Some("".to_string()),
                    attempt: 5,
                    next_poll_at: Some("2025-01-03T00:05:00Z".to_string()),
                    last_error: Some("retrying".to_string()),
                    fallback_used: 0,
                    created_at: "2025-01-03T00:00:00Z".to_string(),
                    updated_at: "2025-01-03T00:10:00Z".to_string(),
                },
            ]
        );

        let mut stmt = conn
            .prepare(
                r#"
                SELECT id, project_id, started_at, version_no, round_no, version_source, version_key
                FROM jobs
                ORDER BY id
                "#,
            )
            .unwrap();
        let migrated_columns: Vec<MigratedJobColumns> = stmt
            .query_map([], |row| {
                Ok(MigratedJobColumns {
                    id: row.get(0)?,
                    project_id: row.get(1)?,
                    started_at: row.get(2)?,
                    version_no: row.get(3)?,
                    round_no: row.get(4)?,
                    version_source: row.get(5)?,
                    version_key: row.get(6)?,
                })
            })
            .unwrap()
            .map(|row| row.unwrap())
            .collect();
        assert_eq!(
            migrated_columns,
            vec![
                MigratedJobColumns {
                    id: "legacy-job-1".to_string(),
                    project_id: "".to_string(),
                    started_at: None,
                    version_no: 1,
                    round_no: 1,
                    version_source: "pdf_hash".to_string(),
                    version_key: "commit-a".to_string(),
                },
                MigratedJobColumns {
                    id: "legacy-job-2".to_string(),
                    project_id: "".to_string(),
                    started_at: None,
                    version_no: 1,
                    round_no: 1,
                    version_source: "pdf_hash".to_string(),
                    version_key: "hash-b".to_string(),
                },
                MigratedJobColumns {
                    id: "legacy-job-3".to_string(),
                    project_id: "".to_string(),
                    started_at: None,
                    version_no: 1,
                    round_no: 1,
                    version_source: "pdf_hash".to_string(),
                    version_key: "hash-c".to_string(),
                },
            ]
        );

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
    fn ensure_schema_migrates_pre_events_project_id_table() {
        #[derive(Debug, PartialEq)]
        struct EventSnapshot {
            id: i64,
            project_id: String,
            job_id: Option<String>,
            event_type: String,
            payload_json: String,
            created_at: String,
        }

        let tmp = tempdir().unwrap();
        let db_path = tmp.path().join("legacy-events.db");

        {
            let conn = rusqlite::Connection::open(&db_path).unwrap();
            conn.execute_batch(
                r#"
                CREATE TABLE jobs (
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
                CREATE TABLE events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                INSERT INTO jobs (id, project_id, paper_id, backend, pdf_path, pdf_hash, status, email, created_at, updated_at)
                VALUES
                    ('job-a', 'proj-a', 'paper-a', 'stanford', 'legacy/a.pdf', 'hash-a', 'COMPLETED', 'a@example.com', '2025-02-01T00:00:00Z', '2025-02-01T00:10:00Z'),
                    ('job-b', 'proj-b', 'paper-b', 'openreview', 'legacy/b.pdf', 'hash-b', 'QUEUED', 'b@example.com', '2025-02-02T00:00:00Z', '2025-02-02T00:10:00Z');
                INSERT INTO events (job_id, event_type, payload_json, created_at)
                VALUES
                    ('job-a', 'job.completed', '{"paper_id":"paper-a","score":8}', '2025-02-01T00:11:00Z'),
                    ('job-b', 'job.queued', '{"paper_id":"paper-b"}', '2025-02-02T00:01:00Z'),
                    (NULL, 'orphan.event', '{"note":"orphan"}', '2025-02-03T00:00:00Z');
                "#,
            )
            .unwrap();
        }

        let db = Db::new_file(db_path);
        db.ensure_schema()
            .expect("ensure_schema must succeed on legacy events db");

        let conn = db.connect().unwrap();
        let mut stmt = conn
            .prepare(
                r#"
                SELECT id, project_id, job_id, event_type, payload_json, created_at
                FROM events
                ORDER BY id
                "#,
            )
            .unwrap();
        let rows: Vec<EventSnapshot> = stmt
            .query_map([], |row| {
                Ok(EventSnapshot {
                    id: row.get(0)?,
                    project_id: row.get(1)?,
                    job_id: row.get(2)?,
                    event_type: row.get(3)?,
                    payload_json: row.get(4)?,
                    created_at: row.get(5)?,
                })
            })
            .unwrap()
            .map(|row| row.unwrap())
            .collect();
        assert_eq!(
            rows,
            vec![
                EventSnapshot {
                    id: 1,
                    project_id: "proj-a".to_string(),
                    job_id: Some("job-a".to_string()),
                    event_type: "job.completed".to_string(),
                    payload_json: r#"{"paper_id":"paper-a","score":8}"#.to_string(),
                    created_at: "2025-02-01T00:11:00Z".to_string(),
                },
                EventSnapshot {
                    id: 2,
                    project_id: "proj-b".to_string(),
                    job_id: Some("job-b".to_string()),
                    event_type: "job.queued".to_string(),
                    payload_json: r#"{"paper_id":"paper-b"}"#.to_string(),
                    created_at: "2025-02-02T00:01:00Z".to_string(),
                },
                EventSnapshot {
                    id: 3,
                    project_id: "".to_string(),
                    job_id: None,
                    event_type: "orphan.event".to_string(),
                    payload_json: r#"{"note":"orphan"}"#.to_string(),
                    created_at: "2025-02-03T00:00:00Z".to_string(),
                },
            ]
        );
    }

    #[test]
    fn update_job_state_rejects_terminal_to_active_transition() {
        let db = Db::new_in_memory("guard_test").unwrap();
        db.ensure_schema().unwrap();

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
        db.ensure_schema().unwrap();

        let job = db.create_job(&make_queued_job("proj", "p2")).unwrap();
        // Queued -> Processing is a valid worker transition.
        db.update_job_state(&job.id, JobStatus::Processing, None, Some(None), None)
            .unwrap();
        // Processing -> Completed is valid.
        db.update_job_state(&job.id, JobStatus::Completed, None, Some(None), None)
            .unwrap();
    }

    #[test]
    fn project_registry_round_trip() {
        let db = Db::new_in_memory("project_registry_test").unwrap();
        db.ensure_schema().unwrap();

        // Empty registry returns None.
        assert!(
            db.resolve_project_config_path("never-seen")
                .unwrap()
                .is_none()
        );

        // Register a path.
        let path = std::path::Path::new("/tmp/project-a/reviewloop.toml");
        db.register_project_config("proj-a", path).unwrap();
        assert_eq!(
            db.resolve_project_config_path("proj-a").unwrap(),
            Some(path.to_path_buf())
        );

        // Re-register with a different path overwrites (eg, repo moved).
        let new_path = std::path::Path::new("/tmp/project-a-renamed/reviewloop.toml");
        db.register_project_config("proj-a", new_path).unwrap();
        assert_eq!(
            db.resolve_project_config_path("proj-a").unwrap(),
            Some(new_path.to_path_buf())
        );

        // Forget removes the row.
        db.forget_project_registration("proj-a").unwrap();
        assert!(db.resolve_project_config_path("proj-a").unwrap().is_none());
    }

    #[test]
    fn project_registry_isolates_different_projects() {
        let db = Db::new_in_memory("project_registry_isolation").unwrap();
        db.ensure_schema().unwrap();

        let path_a = std::path::Path::new("/tmp/a/reviewloop.toml");
        let path_b = std::path::Path::new("/tmp/b/reviewloop.toml");
        db.register_project_config("a", path_a).unwrap();
        db.register_project_config("b", path_b).unwrap();

        assert_eq!(
            db.resolve_project_config_path("a").unwrap(),
            Some(path_a.to_path_buf())
        );
        assert_eq!(
            db.resolve_project_config_path("b").unwrap(),
            Some(path_b.to_path_buf())
        );

        // Forgetting one does not touch the other.
        db.forget_project_registration("a").unwrap();
        assert!(db.resolve_project_config_path("a").unwrap().is_none());
        assert_eq!(
            db.resolve_project_config_path("b").unwrap(),
            Some(path_b.to_path_buf())
        );
    }
}
