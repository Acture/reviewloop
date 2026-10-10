//! Schema migrations up to the current version: the lease columns (v4, OSS-337),
//! review options (v5, OSS-353) and project enablement plus the supervisor row (v6,
//! OSS-338) land on older databases without disturbing existing rows,
//! legacy SUBMITTED rows are settled as UNCERTAIN instead of being resubmitted, concurrent
//! migrations do not collide, and the lease primitives work on a freshly created database.

mod common;

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use common::{PAPER, SUBMIT_TTL, event_types, events_for, load_job};
use reviewloop::{
    db::{ClaimTiming, Db, JobChange, LeaseRecovery, LeaseWrite},
    model::{
        EnqueueMode, EnqueueOutcome, EnqueueRequest, ExistingReason, Job, JobPdf, JobStatus,
        NewJob, SubmitStage, WorkKind,
    },
};
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    sync::Barrier,
    thread,
};

const PROJECT: &str = "project-legacy";
const OTHER_PROJECT: &str = "project-other";
const LEASE_COLUMNS: [&str; 3] = ["lease_owner", "lease_expires_at", "submit_stage"];
/// Added by schema v5 (OSS-353).
const REVIEW_OPTIONS_COLUMN: &str = "review_options";
/// Schema version written by this build (`SCHEMA_VERSION` in src/db.rs).
const CURRENT_SCHEMA_VERSION: i64 = 6;

/// `create_tables_if_missing` + `create_indexes` as of schema v1 (commit 4aeff29),
/// verbatim: `jobs` has no lease columns.
const V1_SCHEMA: &str = r#"
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

    CREATE TABLE IF NOT EXISTS projects (
        project_id   TEXT PRIMARY KEY,
        config_path  TEXT NOT NULL,
        last_seen_at TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_jobs_project_status_next_poll ON jobs(project_id, status, next_poll_at);
    CREATE INDEX IF NOT EXISTS idx_jobs_project_backend_hash ON jobs(project_id, backend, pdf_hash);
    CREATE INDEX IF NOT EXISTS idx_jobs_project_paper_backend ON jobs(project_id, paper_id, backend);
    CREATE INDEX IF NOT EXISTS idx_jobs_project_dedupe ON jobs(project_id, paper_id, backend, pdf_hash, version_key, status);
    CREATE INDEX IF NOT EXISTS idx_events_project_created_at ON events(project_id, created_at);
"#;

const V1_INDEXES: [&str; 5] = [
    "idx_jobs_project_status_next_poll",
    "idx_jobs_project_backend_hash",
    "idx_jobs_project_paper_backend",
    "idx_jobs_project_dedupe",
    "idx_events_project_created_at",
];

/// Columns shared by every legacy row; deliberately non-default so the v1 backfill
/// UPDATEs in `migrate_columns` leave them alone and "intact" means intact.
const LEGACY_BACKEND: &str = "stanford";
const LEGACY_PDF_PATH: &str = "/papers/main.pdf";
const LEGACY_EMAIL: &str = "legacy@example.edu";
const LEGACY_VENUE: &str = "ICLR";
const LEGACY_GIT_TAG: &str = "review-v2";
const LEGACY_GIT_COMMIT: &str = "deadbeefcafe";
const LEGACY_VERSION_NO: u32 = 2;
const LEGACY_ROUND_NO: u32 = 3;
const LEGACY_VERSION_SOURCE: &str = "git_commit";
const LEGACY_CREATED_AT: &str = "2026-01-02T03:04:05+00:00";
const LEGACY_UPDATED_AT: &str = "2026-01-03T04:05:06+00:00";

/// A row as an earlier reviewloop version left it.
struct LegacyRow {
    id: &'static str,
    project_id: &'static str,
    status: JobStatus,
    token: Option<&'static str>,
    attempt: u32,
    started_at: Option<&'static str>,
    next_poll_at: Option<&'static str>,
    last_error: Option<&'static str>,
    fallback_used: bool,
}

impl LegacyRow {
    fn pdf_hash(&self) -> String {
        format!("hash-{}", self.id)
    }
}

const LEGACY_QUEUED: LegacyRow = LegacyRow {
    id: "legacy-queued",
    project_id: PROJECT,
    status: JobStatus::Queued,
    token: None,
    attempt: 1,
    started_at: None,
    next_poll_at: None,
    last_error: Some("rate limited by provider"),
    fallback_used: false,
};

const LEGACY_SUBMITTED: LegacyRow = LegacyRow {
    id: "legacy-submitted",
    project_id: PROJECT,
    status: JobStatus::Submitted,
    token: None,
    attempt: 2,
    started_at: Some("2026-01-03T04:00:00+00:00"),
    next_poll_at: Some("2026-01-03T05:00:00+00:00"),
    last_error: None,
    fallback_used: true,
};

const LEGACY_PROCESSING: LegacyRow = LegacyRow {
    id: "legacy-processing",
    project_id: PROJECT,
    status: JobStatus::Processing,
    token: Some("tok-legacy-123"),
    attempt: 3,
    started_at: Some("2026-01-03T04:00:00+00:00"),
    next_poll_at: Some("2026-01-03T06:00:00+00:00"),
    last_error: Some("poll returned 502"),
    fallback_used: false,
};

/// A legacy SUBMITTED row in another project, to check recovery stays project-scoped.
const OTHER_SUBMITTED: LegacyRow = LegacyRow {
    id: "other-submitted",
    project_id: OTHER_PROJECT,
    status: JobStatus::Submitted,
    token: None,
    attempt: 0,
    started_at: None,
    next_poll_at: None,
    last_error: None,
    fallback_used: false,
};

const LEGACY_ROWS: [&LegacyRow; 4] = [
    &LEGACY_QUEUED,
    &LEGACY_SUBMITTED,
    &LEGACY_PROCESSING,
    &OTHER_SUBMITTED,
];

const LEGACY_EVENT_TYPE: &str = "legacy_submitted";

fn ts(value: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(value)
        .with_context(|| format!("bad test timestamp {value}"))?
        .with_timezone(&Utc))
}

fn opt_ts(value: Option<&str>) -> Result<Option<DateTime<Utc>>> {
    value.map(ts).transpose()
}

/// Hand-build a schema-v1 database at `path` holding [`LEGACY_ROWS`] and one event,
/// in `journal_mode` (v1's `ensure_schema` always set WAL). The connection is closed
/// before returning.
fn build_v1_database(path: &Path, journal_mode: &str) -> Result<()> {
    let conn = Connection::open(path)?;
    let mode: String = conn.query_row(
        &format!("PRAGMA journal_mode = {journal_mode}"),
        [],
        |row| row.get(0),
    )?;
    assert!(
        mode.eq_ignore_ascii_case(journal_mode),
        "test setup: journal_mode {journal_mode} not applied (got {mode})"
    );
    conn.execute_batch(V1_SCHEMA)?;
    for row in LEGACY_ROWS {
        conn.execute(
            r#"
            INSERT INTO jobs (
                id, project_id, paper_id, backend, pdf_path, pdf_hash, status, token, email, venue,
                git_tag, git_commit, version_no, round_no, version_source, version_key,
                attempt, started_at, next_poll_at, last_error, fallback_used, created_at, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23)
            "#,
            params![
                row.id,
                row.project_id,
                PAPER,
                LEGACY_BACKEND,
                LEGACY_PDF_PATH,
                row.pdf_hash(),
                row.status.as_str(),
                row.token,
                LEGACY_EMAIL,
                LEGACY_VENUE,
                LEGACY_GIT_TAG,
                LEGACY_GIT_COMMIT,
                LEGACY_VERSION_NO,
                LEGACY_ROUND_NO,
                LEGACY_VERSION_SOURCE,
                LEGACY_GIT_COMMIT,
                row.attempt,
                row.started_at,
                row.next_poll_at,
                row.last_error,
                i64::from(row.fallback_used),
                LEGACY_CREATED_AT,
                LEGACY_UPDATED_AT,
            ],
        )?;
    }
    conn.execute(
        r#"
        INSERT INTO events(project_id, job_id, event_type, payload_json, created_at)
        VALUES (?1, ?2, ?3, ?4, ?5)
        "#,
        params![
            PROJECT,
            LEGACY_PROCESSING.id,
            LEGACY_EVENT_TYPE,
            json!({ "token": LEGACY_PROCESSING.token }).to_string(),
            LEGACY_UPDATED_AT,
        ],
    )?;
    conn.pragma_update(None, "user_version", 1)?;
    drop(conn);

    assert_eq!(
        user_version(path)?,
        1,
        "test setup: v1 file not at version 1"
    );
    let columns = jobs_columns(path)?;
    for column in LEASE_COLUMNS {
        assert!(
            columns.iter().all(|info| info.name != column),
            "test setup: v1 jobs table must not have {column}"
        );
    }
    Ok(())
}

fn user_version(path: &Path) -> Result<i64> {
    let conn = Connection::open(path)?;
    Ok(conn.query_row("PRAGMA user_version", [], |row| row.get(0))?)
}

#[derive(Debug)]
struct ColumnInfo {
    name: String,
    decl_type: String,
    not_null: bool,
    default: Option<String>,
}

fn jobs_columns(path: &Path) -> Result<Vec<ColumnInfo>> {
    let conn = Connection::open(path)?;
    let mut stmt = conn.prepare("PRAGMA table_info(jobs)")?;
    let rows = stmt.query_map([], |row| {
        Ok(ColumnInfo {
            name: row.get(1)?,
            decl_type: row.get(2)?,
            not_null: row.get::<_, i64>(3)? != 0,
            default: row.get(4)?,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

fn index_names(path: &Path) -> Result<Vec<String>> {
    let conn = Connection::open(path)?;
    let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type = 'index'")?;
    let rows = stmt.query_map([], |row| row.get(0))?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

/// Raw (owner, expires_at, stage) as stored, bypassing the crate's row mapping.
fn raw_lease_columns(
    path: &Path,
    job_id: &str,
) -> Result<(Option<String>, Option<String>, Option<String>)> {
    let conn = Connection::open(path)?;
    Ok(conn.query_row(
        "SELECT lease_owner, lease_expires_at, submit_stage FROM jobs WHERE id = ?1",
        params![job_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?)
}

/// Raw `jobs.review_options` as stored, bypassing the crate's row mapping.
fn raw_review_options(path: &Path, job_id: &str) -> Result<Option<String>> {
    let conn = Connection::open(path)?;
    Ok(conn.query_row(
        "SELECT review_options FROM jobs WHERE id = ?1",
        params![job_id],
        |row| row.get(0),
    )?)
}

/// Each column added after v1 that the current schema needs (the lease columns and
/// review options) exists exactly once, as a nullable TEXT column with no default.
fn assert_current_columns(path: &Path) -> Result<()> {
    let columns = jobs_columns(path)?;
    for column in LEASE_COLUMNS.into_iter().chain([REVIEW_OPTIONS_COLUMN]) {
        let matches: Vec<&ColumnInfo> = columns.iter().filter(|c| c.name == column).collect();
        assert_eq!(matches.len(), 1, "jobs.{column} must exist exactly once");
        let info = matches[0];
        assert_eq!(info.decl_type, "TEXT", "jobs.{column} type: {info:?}");
        assert!(!info.not_null, "jobs.{column} must be nullable: {info:?}");
        assert_eq!(info.default, None, "jobs.{column} default: {info:?}");
    }
    Ok(())
}

/// Every v1 column survives the migration and the lease fields and review options
/// start empty.
fn assert_legacy_row_intact(job: &Job, row: &LegacyRow) -> Result<()> {
    let id = row.id;
    assert_eq!(job.id, id);
    assert_eq!(job.project_id, row.project_id, "{id}: project_id");
    assert_eq!(job.paper_id, PAPER, "{id}: paper_id");
    assert_eq!(job.backend, LEGACY_BACKEND, "{id}: backend");
    assert_eq!(job.pdf_path, LEGACY_PDF_PATH, "{id}: pdf_path");
    assert_eq!(job.pdf_hash, row.pdf_hash(), "{id}: pdf_hash");
    assert_eq!(job.status, row.status, "{id}: status");
    assert_eq!(job.token.as_deref(), row.token, "{id}: token");
    assert_eq!(job.email, LEGACY_EMAIL, "{id}: email");
    assert_eq!(job.venue.as_deref(), Some(LEGACY_VENUE), "{id}: venue");
    assert_eq!(
        job.git_tag.as_deref(),
        Some(LEGACY_GIT_TAG),
        "{id}: git_tag"
    );
    assert_eq!(
        job.git_commit.as_deref(),
        Some(LEGACY_GIT_COMMIT),
        "{id}: git_commit"
    );
    assert_eq!(job.version_no, LEGACY_VERSION_NO, "{id}: version_no");
    assert_eq!(job.round_no, LEGACY_ROUND_NO, "{id}: round_no");
    assert_eq!(
        job.version_source, LEGACY_VERSION_SOURCE,
        "{id}: version_source"
    );
    assert_eq!(job.version_key, LEGACY_GIT_COMMIT, "{id}: version_key");
    assert_eq!(job.attempt, row.attempt, "{id}: attempt");
    assert_eq!(job.started_at, opt_ts(row.started_at)?, "{id}: started_at");
    assert_eq!(
        job.next_poll_at,
        opt_ts(row.next_poll_at)?,
        "{id}: next_poll_at"
    );
    assert_eq!(
        job.last_error.as_deref(),
        row.last_error,
        "{id}: last_error"
    );
    assert_eq!(job.fallback_used, row.fallback_used, "{id}: fallback_used");
    assert_eq!(job.created_at, ts(LEGACY_CREATED_AT)?, "{id}: created_at");
    assert_eq!(job.updated_at, ts(LEGACY_UPDATED_AT)?, "{id}: updated_at");
    assert_eq!(job.lease_owner, None, "{id}: lease_owner");
    assert_eq!(job.lease_expires_at, None, "{id}: lease_expires_at");
    assert_eq!(job.submit_stage, None, "{id}: submit_stage");
    assert!(
        job.review_options.is_empty(),
        "{id}: review_options {:?}",
        job.review_options
    );
    Ok(())
}

/// A v1 file migrated by one `ensure_schema` call.
struct MigratedV1 {
    _tmp: tempfile::TempDir,
    path: PathBuf,
    db: Db,
}

fn migrated_v1() -> Result<MigratedV1> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("reviewloop.db");
    build_v1_database(&path, "wal")?;
    let db = Db::new_file(path.clone());
    db.ensure_schema()?;
    Ok(MigratedV1 {
        _tmp: tmp,
        path,
        db,
    })
}

/// Run `ensure_schema` from `threads` threads at once, each opening its own `Db`
/// handle after a shared barrier so file creation and migration genuinely overlap.
fn ensure_schema_concurrently(path: &Path, threads: usize) -> Vec<String> {
    let barrier = Barrier::new(threads);
    thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    Db::new_file(path.to_path_buf()).ensure_schema()
                })
            })
            .collect();
        handles
            .into_iter()
            .filter_map(|handle| match handle.join() {
                Ok(Ok(())) => None,
                Ok(Err(err)) => Some(format!("{err:#}")),
                Err(_) => Some("ensure_schema thread panicked".to_string()),
            })
            .collect()
    })
}

fn assert_v1_file_migrates_concurrently(journal_mode: &str, rounds: usize) -> Result<()> {
    const THREADS: usize = 4;
    for round in 0..rounds {
        let tmp = tempfile::tempdir()?;
        let path = tmp.path().join("reviewloop.db");
        build_v1_database(&path, journal_mode)?;

        let errors = ensure_schema_concurrently(&path, THREADS);
        assert!(
            errors.is_empty(),
            "{journal_mode} round {round}: concurrent ensure_schema failed: {errors:?}"
        );
        assert_eq!(
            user_version(&path)?,
            CURRENT_SCHEMA_VERSION,
            "{journal_mode} round {round}: user_version"
        );
        assert_current_columns(&path)?;

        let db = Db::new_file(path.clone());
        for row in LEGACY_ROWS {
            assert_legacy_row_intact(&load_job(&db, row.id)?, row)?;
        }
        assert_eq!(
            event_types(&db.list_timeline_events(PROJECT, PAPER)?),
            vec![LEGACY_EVENT_TYPE],
            "{journal_mode} round {round}: migration must not write events"
        );
    }
    Ok(())
}

fn fresh_new_job(paper_id: &str) -> NewJob {
    NewJob {
        project_id: PROJECT.to_string(),
        paper_id: paper_id.to_string(),
        backend: "stanford".to_string(),
        pdf: JobPdf::Unpinned {
            pdf_path: "/papers/fresh.pdf".to_string(),
            pdf_hash: "fresh-hash".to_string(),
        },
        status: JobStatus::Queued,
        email: "fresh@example.edu".to_string(),
        venue: None,
        review_options: Default::default(),
        git_tag: None,
        git_commit: None,
        next_poll_at: None,
    }
}

#[test]
fn v1_database_migrates_to_current_and_keeps_every_legacy_row_intact() -> Result<()> {
    let MigratedV1 { path, db, _tmp } = migrated_v1()?;

    assert_eq!(user_version(&path)?, CURRENT_SCHEMA_VERSION);
    assert_current_columns(&path)?;
    let indexes = index_names(&path)?;
    for index in V1_INDEXES {
        assert!(
            indexes.iter().any(|name| name == index),
            "index {index} lost: {indexes:?}"
        );
    }

    for row in LEGACY_ROWS {
        assert_legacy_row_intact(&load_job(&db, row.id)?, row)?;
        assert_eq!(
            raw_lease_columns(&path, row.id)?,
            (None, None, None),
            "{}: raw lease columns must be NULL",
            row.id
        );
        assert_eq!(
            raw_review_options(&path, row.id)?,
            None,
            "{}: raw review_options must be NULL",
            row.id
        );
    }

    let events = events_for(&db, PROJECT, LEGACY_PROCESSING.id)?;
    assert_eq!(event_types(&events), vec![LEGACY_EVENT_TYPE]);
    assert_eq!(events[0].payload["token"], json!(LEGACY_PROCESSING.token));
    assert_eq!(events[0].project_id, PROJECT);

    // Already current: a second call writes nothing.
    db.ensure_schema()?;
    assert_eq!(user_version(&path)?, CURRENT_SCHEMA_VERSION);
    assert_current_columns(&path)?;
    for row in LEGACY_ROWS {
        assert_legacy_row_intact(&load_job(&db, row.id)?, row)?;
    }
    assert_eq!(
        event_types(&db.list_timeline_events(PROJECT, PAPER)?),
        vec![LEGACY_EVENT_TYPE]
    );
    Ok(())
}

#[test]
fn legacy_submitted_row_becomes_uncertain_once_and_is_never_resubmitted() -> Result<()> {
    let MigratedV1 { path, db, _tmp } = migrated_v1()?;
    let before = load_job(&db, LEGACY_SUBMITTED.id)?;
    let now = Utc::now();

    // Before recovery it is already out of reach of the submit path.
    assert!(
        db.claim_job(
            LEGACY_SUBMITTED.id,
            WorkKind::Submit,
            ClaimTiming::Now,
            now,
            SUBMIT_TTL
        )?
        .is_none(),
        "a legacy SUBMITTED row must not be claimable for submit"
    );

    let report = db.recover_expired_leases(PROJECT, now)?;
    assert_eq!(
        report,
        LeaseRecovery {
            released_claims: 0,
            uncertain_submits: 1
        }
    );

    let job = load_job(&db, LEGACY_SUBMITTED.id)?;
    assert_eq!(job.status, JobStatus::Submitted);
    assert_eq!(job.submit_stage, Some(SubmitStage::Uncertain));
    assert_eq!(job.lease_owner, None);
    assert_eq!(job.lease_expires_at, None);
    assert_eq!(job.token, None);
    assert_eq!(job.attempt, LEGACY_SUBMITTED.attempt);
    assert_eq!(job.next_poll_at, opt_ts(LEGACY_SUBMITTED.next_poll_at)?);
    assert_eq!(job.started_at, opt_ts(LEGACY_SUBMITTED.started_at)?);
    assert!(job.fallback_used, "fallback_used must be preserved");
    let last_error = job
        .last_error
        .clone()
        .context("UNCERTAIN needs a diagnostic")?;
    assert!(
        last_error.starts_with("submission outcome unknown:"),
        "{last_error}"
    );
    assert!(
        last_error.contains("earlier reviewloop version"),
        "{last_error}"
    );
    assert!(
        last_error.contains("provider may have accepted it"),
        "{last_error}"
    );
    assert!(
        last_error.ends_with(&before.reconcile_hint()),
        "{last_error}"
    );
    assert!(last_error.contains("import-token"), "{last_error}");
    assert_eq!(
        raw_lease_columns(&path, LEGACY_SUBMITTED.id)?,
        (None, None, Some("UNCERTAIN".to_string()))
    );

    let events = events_for(&db, PROJECT, LEGACY_SUBMITTED.id)?;
    assert_eq!(event_types(&events), vec!["submit_outcome_unknown"]);
    let payload = &events[0].payload;
    assert_eq!(payload["source"], json!("legacy_submitted"));
    assert_eq!(payload["previous_owner"], Value::Null);
    assert_eq!(payload["reason"], json!(last_error));
    assert_eq!(events[0].project_id, PROJECT);

    // Other rows of the project, and the other project's SUBMITTED row, are untouched.
    for row in [&LEGACY_QUEUED, &LEGACY_PROCESSING, &OTHER_SUBMITTED] {
        assert_legacy_row_intact(&load_job(&db, row.id)?, row)?;
    }
    assert!(events_for(&db, OTHER_PROJECT, OTHER_SUBMITTED.id)?.is_empty());

    // Never handed back to the submit path.
    assert!(
        db.list_ready_queued(PROJECT, 10, now)?
            .iter()
            .all(|queued| queued.id != LEGACY_SUBMITTED.id)
    );
    assert!(
        db.claim_job(
            LEGACY_SUBMITTED.id,
            WorkKind::Submit,
            ClaimTiming::Now,
            now,
            SUBMIT_TTL
        )?
        .is_none()
    );

    // A second pass, even much later, is a no-op.
    let later = now + Duration::hours(2);
    assert_eq!(
        db.recover_expired_leases(PROJECT, later)?,
        LeaseRecovery::default()
    );
    let again = load_job(&db, LEGACY_SUBMITTED.id)?;
    assert_eq!(
        again.updated_at, job.updated_at,
        "second pass rewrote the row"
    );
    assert_eq!(again.last_error, job.last_error);
    assert_eq!(again.submit_stage, Some(SubmitStage::Uncertain));
    assert_eq!(
        event_types(&events_for(&db, PROJECT, LEGACY_SUBMITTED.id)?),
        vec!["submit_outcome_unknown"]
    );

    // The other project is settled only by its own pass.
    assert_eq!(
        db.recover_expired_leases(OTHER_PROJECT, now)?,
        LeaseRecovery {
            released_claims: 0,
            uncertain_submits: 1
        }
    );
    let other = load_job(&db, OTHER_SUBMITTED.id)?;
    assert_eq!(other.status, JobStatus::Submitted);
    assert_eq!(other.submit_stage, Some(SubmitStage::Uncertain));
    let other_events = events_for(&db, OTHER_PROJECT, OTHER_SUBMITTED.id)?;
    assert_eq!(event_types(&other_events), vec!["submit_outcome_unknown"]);
    assert_eq!(other_events[0].payload["source"], json!("legacy_submitted"));
    Ok(())
}

#[test]
fn legacy_queued_and_processing_rows_are_claimable_after_migration() -> Result<()> {
    let MigratedV1 { path, db, _tmp } = migrated_v1()?;
    let now = Utc::now();
    let submit_ttl = SUBMIT_TTL;
    let poll_ttl = Duration::minutes(10);

    let submit = db
        .claim_job(
            LEGACY_QUEUED.id,
            WorkKind::Submit,
            ClaimTiming::WhenDue,
            now,
            submit_ttl,
        )?
        .context("legacy QUEUED row must be claimable for submit")?;
    assert_eq!(submit.kind, WorkKind::Submit);
    assert_eq!(submit.expires_at, now + submit_ttl);
    let queued = load_job(&db, LEGACY_QUEUED.id)?;
    assert_eq!(queued.status, JobStatus::Queued);
    assert_eq!(queued.submit_stage, Some(SubmitStage::Claimed));
    assert_eq!(queued.lease_owner.as_deref(), Some(submit.owner.as_str()));
    assert_eq!(queued.lease_expires_at, Some(now + submit_ttl));
    assert_eq!(queued.attempt, LEGACY_QUEUED.attempt);
    assert_eq!(queued.last_error.as_deref(), LEGACY_QUEUED.last_error);
    assert_eq!(
        raw_lease_columns(&path, LEGACY_QUEUED.id)?.2.as_deref(),
        Some("CLAIMED")
    );

    let poll = db
        .claim_job(
            LEGACY_PROCESSING.id,
            WorkKind::Poll,
            ClaimTiming::WhenDue,
            now,
            poll_ttl,
        )?
        .context("legacy PROCESSING row with a token must be claimable for poll")?;
    let processing = load_job(&db, LEGACY_PROCESSING.id)?;
    assert_eq!(processing.status, JobStatus::Processing);
    assert_eq!(processing.submit_stage, None);
    assert_eq!(processing.lease_owner.as_deref(), Some(poll.owner.as_str()));
    assert_eq!(processing.lease_expires_at, Some(now + poll_ttl));
    assert_eq!(processing.token.as_deref(), LEGACY_PROCESSING.token);

    // Neither kind of claim applies to the legacy SUBMITTED row.
    for kind in [WorkKind::Submit, WorkKind::Poll] {
        assert!(
            db.claim_job(LEGACY_SUBMITTED.id, kind, ClaimTiming::Now, now, submit_ttl)?
                .is_none(),
            "legacy SUBMITTED row claimed for {kind:?}"
        );
    }
    Ok(())
}

#[test]
fn concurrent_ensure_schema_on_wal_v1_file_migrates_without_duplicate_columns() -> Result<()> {
    assert_v1_file_migrates_concurrently("wal", 12)
}

#[test]
fn concurrent_ensure_schema_on_rollback_journal_v1_file_migrates_without_duplicate_columns()
-> Result<()> {
    assert_v1_file_migrates_concurrently("delete", 12)
}

#[test]
fn concurrent_ensure_schema_on_missing_file_creates_current_schema() -> Result<()> {
    for round in 0..8 {
        let tmp = tempfile::tempdir()?;
        let path = tmp.path().join("reviewloop.db");
        let errors = ensure_schema_concurrently(&path, 4);
        assert!(
            errors.is_empty(),
            "round {round}: concurrent ensure_schema on a new file failed: {errors:?}"
        );
        assert_eq!(
            user_version(&path)?,
            CURRENT_SCHEMA_VERSION,
            "round {round}: user_version"
        );
        assert_current_columns(&path)?;
    }
    Ok(())
}

#[test]
fn fresh_database_has_lease_columns_and_supports_claim_and_finish() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("fresh.db");
    assert!(!path.exists());
    let db = Db::new_file(path.clone());
    db.ensure_schema()?;
    assert_eq!(user_version(&path)?, CURRENT_SCHEMA_VERSION);
    assert_current_columns(&path)?;

    let job = db.create_job(&fresh_new_job(PAPER))?;
    assert_eq!(job.status, JobStatus::Queued);
    assert_eq!(job.lease_owner, None);
    assert_eq!(job.lease_expires_at, None);
    assert_eq!(job.submit_stage, None);

    // Claim: the owner, expiry and CLAIMED stage are persisted.
    let now = Utc::now();
    let ttl = SUBMIT_TTL;
    let lease = db
        .claim_job(&job.id, WorkKind::Submit, ClaimTiming::WhenDue, now, ttl)?
        .context("fresh QUEUED job must be claimable")?;
    assert_eq!(lease.kind, WorkKind::Submit);
    assert_eq!(lease.expires_at, now + ttl);
    assert_eq!(lease.job.lease_owner.as_deref(), Some(lease.owner.as_str()));
    assert_eq!(lease.job.submit_stage, Some(SubmitStage::Claimed));
    let stored = load_job(&db, &job.id)?;
    assert_eq!(stored.lease_owner.as_deref(), Some(lease.owner.as_str()));
    assert_eq!(stored.lease_expires_at, Some(now + ttl));
    assert_eq!(stored.submit_stage, Some(SubmitStage::Claimed));
    let (raw_owner, raw_expires, raw_stage) = raw_lease_columns(&path, &job.id)?;
    assert_eq!(raw_owner.as_deref(), Some(lease.owner.as_str()));
    assert!(raw_expires.is_some());
    assert_eq!(raw_stage.as_deref(), Some("CLAIMED"));

    // A live lease excludes every other claimant.
    for timing in [ClaimTiming::WhenDue, ClaimTiming::Now] {
        assert!(
            db.claim_job(&job.id, WorkKind::Submit, timing, now, ttl)?
                .is_none(),
            "second claim with {timing:?} must fail while the lease is live"
        );
    }

    // Finish under the lease: change applied, lease released, event written.
    let finished_at = now + Duration::minutes(1);
    let cooldown = now + Duration::minutes(5);
    let write = db.finish_lease(
        &lease,
        finished_at,
        &JobChange {
            status: JobStatus::Queued,
            attempt: Some(1),
            next_poll_at: Some(Some(cooldown)),
            last_error: Some(Some("rate limited".to_string())),
            submit_stage: None,
            fallback_used: None,
        },
        "submit_rate_limited",
        json!({ "retry_after_minutes": 5 }),
    )?;
    assert_eq!(write, LeaseWrite::Applied);
    let finished = load_job(&db, &job.id)?;
    assert_eq!(finished.status, JobStatus::Queued);
    assert_eq!(finished.attempt, 1);
    assert_eq!(finished.next_poll_at, Some(cooldown));
    assert_eq!(finished.last_error.as_deref(), Some("rate limited"));
    assert_eq!(finished.lease_owner, None);
    assert_eq!(finished.lease_expires_at, None);
    assert_eq!(finished.submit_stage, None);
    let events = events_for(&db, PROJECT, &job.id)?;
    assert_eq!(event_types(&events), vec!["submit_rate_limited"]);
    assert_eq!(events[0].payload["retry_after_minutes"], json!(5));

    // The cooldown holds for a due-only claim, then lapses.
    assert!(
        db.claim_job(
            &job.id,
            WorkKind::Submit,
            ClaimTiming::WhenDue,
            finished_at,
            ttl
        )?
        .is_none(),
        "claim inside the cooldown must fail"
    );
    let second = db
        .claim_job(
            &job.id,
            WorkKind::Submit,
            ClaimTiming::WhenDue,
            cooldown,
            ttl,
        )?
        .context("claim after the cooldown must succeed")?;
    assert_ne!(second.owner, lease.owner);

    // An expired owner's result is rejected and nothing is written.
    let too_late = cooldown + ttl + Duration::minutes(1);
    let lost = db.finish_lease(
        &second,
        too_late,
        &JobChange {
            status: JobStatus::Failed,
            attempt: Some(2),
            next_poll_at: None,
            last_error: Some(Some("late failure".to_string())),
            submit_stage: None,
            fallback_used: None,
        },
        "submit_failed",
        json!({}),
    )?;
    assert_eq!(lost, LeaseWrite::Lost(Some(JobStatus::Queued)));
    let after_lost = load_job(&db, &job.id)?;
    assert_eq!(after_lost.status, JobStatus::Queued);
    assert_eq!(after_lost.attempt, 1);
    assert_eq!(after_lost.last_error.as_deref(), Some("rate limited"));
    assert_eq!(
        after_lost.lease_owner.as_deref(),
        Some(second.owner.as_str())
    );
    assert_eq!(after_lost.submit_stage, Some(SubmitStage::Claimed));
    assert_eq!(
        event_types(&events_for(&db, PROJECT, &job.id)?),
        vec!["submit_rate_limited"]
    );

    // Recovery returns the expired pre-dispatch claim to the queue.
    assert_eq!(
        db.recover_expired_leases(PROJECT, too_late)?,
        LeaseRecovery {
            released_claims: 1,
            uncertain_submits: 0
        }
    );
    let released = load_job(&db, &job.id)?;
    assert_eq!(released.status, JobStatus::Queued);
    assert_eq!(released.lease_owner, None);
    assert_eq!(released.lease_expires_at, None);
    assert_eq!(released.submit_stage, None);
    assert_eq!(released.next_poll_at, Some(cooldown));
    let events = events_for(&db, PROJECT, &job.id)?;
    assert_eq!(
        event_types(&events),
        vec!["submit_rate_limited", "submit_claim_expired"]
    );
    assert_eq!(events[1].payload["previous_owner"], json!(second.owner));
    Ok(())
}

/// `enqueue_requests` as added by schema v2 (OSS-336), before the lease columns.
const V2_ENQUEUE_REQUESTS: &str = r#"
    CREATE TABLE IF NOT EXISTS enqueue_requests (
        project_id    TEXT NOT NULL,
        request_key   TEXT NOT NULL,
        identity_json TEXT NOT NULL,
        job_id        TEXT NOT NULL,
        created_at    TEXT NOT NULL,
        PRIMARY KEY (project_id, request_key)
    );
"#;

#[test]
fn v2_database_gains_lease_columns_and_keeps_enqueue_requests() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("reviewloop.db");
    build_v1_database(&path, "wal")?;
    {
        let conn = Connection::open(&path)?;
        conn.execute_batch(V2_ENQUEUE_REQUESTS)?;
        conn.execute(
            "INSERT INTO enqueue_requests VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                PROJECT,
                "key-1",
                "{}",
                LEGACY_PROCESSING.id,
                LEGACY_CREATED_AT
            ],
        )?;
        conn.pragma_update(None, "user_version", 2)?;
    }

    let db = Db::new_file(path.clone());
    db.ensure_schema()?;

    assert_eq!(user_version(&path)?, CURRENT_SCHEMA_VERSION);
    assert_current_columns(&path)?;
    for row in LEGACY_ROWS {
        assert_legacy_row_intact(&load_job(&db, row.id)?, row)?;
    }
    let bound: String = Connection::open(&path)?.query_row(
        "SELECT job_id FROM enqueue_requests WHERE project_id = ?1 AND request_key = ?2",
        params![PROJECT, "key-1"],
        |row| row.get(0),
    )?;
    assert_eq!(bound, LEGACY_PROCESSING.id);
    Ok(())
}

/// Two schema-v3 lineages exist: master's (OSS-335 `snapshot_path`, no lease columns)
/// and OSS-337's pre-merge one (lease columns, no `snapshot_path`). Both reach the
/// current schema with every column, so neither build's v3 can hide the other's
/// migration.
fn v3_database_upgrades_with_every_column(add: &[&str]) -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("reviewloop.db");
    build_v1_database(&path, "wal")?;
    {
        let conn = Connection::open(&path)?;
        conn.execute_batch(V2_ENQUEUE_REQUESTS)?;
        for column in add {
            conn.execute_batch(&format!("ALTER TABLE jobs ADD COLUMN {column} TEXT"))?;
        }
        conn.pragma_update(None, "user_version", 3)?;
    }

    let db = Db::new_file(path.clone());
    db.ensure_schema()?;

    assert_eq!(user_version(&path)?, CURRENT_SCHEMA_VERSION);
    assert_current_columns(&path)?;
    let columns = jobs_columns(&path)?;
    assert!(columns.iter().any(|info| info.name == "snapshot_path"));
    for row in LEGACY_ROWS {
        assert_legacy_row_intact(&load_job(&db, row.id)?, row)?;
    }
    Ok(())
}

#[test]
fn master_v3_database_gains_lease_columns() -> Result<()> {
    v3_database_upgrades_with_every_column(&["snapshot_path"])
}

#[test]
fn branch_v3_database_gains_snapshot_path() -> Result<()> {
    v3_database_upgrades_with_every_column(&LEASE_COLUMNS)
}

/// A schema-v4 database (OSS-337): the v1 rows plus `enqueue_requests`, `snapshot_path`
/// and the lease columns, without `review_options`.
fn build_v4_database(path: &Path) -> Result<()> {
    build_v1_database(path, "wal")?;
    let conn = Connection::open(path)?;
    conn.execute_batch(V2_ENQUEUE_REQUESTS)?;
    for column in ["snapshot_path"].into_iter().chain(LEASE_COLUMNS) {
        conn.execute_batch(&format!("ALTER TABLE jobs ADD COLUMN {column} TEXT"))?;
    }
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_enqueue_requests_job ON enqueue_requests(job_id);",
    )?;
    conn.pragma_update(None, "user_version", 4)?;
    drop(conn);

    assert_eq!(
        user_version(path)?,
        4,
        "test setup: v4 file not at version 4"
    );
    assert!(
        jobs_columns(path)?
            .iter()
            .all(|info| info.name != REVIEW_OPTIONS_COLUMN),
        "test setup: v4 jobs table must not have {REVIEW_OPTIONS_COLUMN}"
    );
    Ok(())
}

/// `identity_json` for `row` exactly as builds before OSS-353 wrote it: no
/// `review_options` field.
fn pre_v5_identity_json(row: &LegacyRow) -> String {
    format!(
        r#"{{"paper_id":"{PAPER}","backend":"{LEGACY_BACKEND}","pdf_hash":"{}","venue":"{LEGACY_VENUE}","version_source":"{LEGACY_VERSION_SOURCE}","version_key":"{LEGACY_GIT_COMMIT}"}}"#,
        row.pdf_hash()
    )
}

/// The request that would have produced `row`: same manuscript, venue and commit.
fn legacy_request(row: &LegacyRow, request_key: Option<&str>) -> EnqueueRequest {
    EnqueueRequest {
        job: NewJob {
            project_id: row.project_id.to_string(),
            paper_id: PAPER.to_string(),
            backend: LEGACY_BACKEND.to_string(),
            pdf: JobPdf::Unpinned {
                pdf_path: LEGACY_PDF_PATH.to_string(),
                pdf_hash: row.pdf_hash(),
            },
            status: JobStatus::Queued,
            email: LEGACY_EMAIL.to_string(),
            venue: Some(LEGACY_VENUE.to_string()),
            review_options: Default::default(),
            git_tag: Some(LEGACY_GIT_TAG.to_string()),
            git_commit: Some(LEGACY_GIT_COMMIT.to_string()),
            next_poll_at: None,
        },
        request_key: request_key.map(str::to_string),
        mode: EnqueueMode::Deduplicate,
        source: "test".to_string(),
    }
}

#[test]
fn v4_database_gains_review_options_and_replays_pre_upgrade_request_keys() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("reviewloop.db");
    build_v4_database(&path)?;
    Connection::open(&path)?.execute(
        "INSERT INTO enqueue_requests VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            PROJECT,
            "key-v4",
            pre_v5_identity_json(&LEGACY_QUEUED),
            LEGACY_QUEUED.id,
            LEGACY_CREATED_AT
        ],
    )?;

    let db = Db::new_file(path.clone());
    db.ensure_schema()?;

    assert_eq!(user_version(&path)?, CURRENT_SCHEMA_VERSION);
    assert_current_columns(&path)?;
    for row in LEGACY_ROWS {
        assert_legacy_row_intact(&load_job(&db, row.id)?, row)?;
        assert_eq!(
            raw_review_options(&path, row.id)?,
            None,
            "{}: raw review_options must be NULL",
            row.id
        );
    }
    assert_eq!(
        event_types(&db.list_timeline_events(PROJECT, PAPER)?),
        vec![LEGACY_EVENT_TYPE],
        "migration must not write events"
    );

    // The key bound before the upgrade replays to its job instead of failing as a
    // corrupt identity or conflicting on the new field.
    match db.enqueue(&legacy_request(&LEGACY_QUEUED, Some("key-v4")))? {
        EnqueueOutcome::Existing {
            job,
            reason: ExistingReason::RequestReplay,
        } => assert_legacy_row_intact(&job, &LEGACY_QUEUED)?,
        other => panic!("expected a replay of {}, got {other:?}", LEGACY_QUEUED.id),
    }

    // A legacy row's NULL review options cover a request without options.
    match db.enqueue(&legacy_request(&LEGACY_PROCESSING, None))? {
        EnqueueOutcome::Existing {
            job,
            reason: ExistingReason::Covered,
        } => assert_eq!(job.id, LEGACY_PROCESSING.id),
        other => panic!("expected {} to cover, got {other:?}", LEGACY_PROCESSING.id),
    }

    let jobs: i64 =
        Connection::open(&path)?.query_row("SELECT COUNT(*) FROM jobs", [], |row| row.get(0))?;
    assert_eq!(jobs, LEGACY_ROWS.len() as i64, "nothing new was enqueued");
    Ok(())
}

/// A schema-v5 database (OSS-353): v4 plus `jobs.review_options`; the
/// `projects` table still has only its three original columns.
fn build_v5_database(path: &Path) -> Result<()> {
    build_v4_database(path)?;
    let conn = Connection::open(path)?;
    conn.execute_batch(&format!(
        "ALTER TABLE jobs ADD COLUMN {REVIEW_OPTIONS_COLUMN} TEXT"
    ))?;
    conn.pragma_update(None, "user_version", 5)?;
    Ok(())
}

/// Registry rows from before OSS-338 come through disabled and undecided:
/// nothing starts running until someone enables it, and the one-time
/// migration of a single-project daemon install may still enable its project.
#[test]
fn v5_database_keeps_registered_projects_disabled_and_undecided() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("reviewloop.db");
    build_v5_database(&path)?;
    let seen_at = "2026-05-01T10:00:00+00:00";
    {
        let conn = Connection::open(&path)?;
        for (project_id, config_path) in [
            (PROJECT, "/repos/legacy/reviewloop.toml"),
            (OTHER_PROJECT, "/repos/other/reviewloop.toml"),
        ] {
            conn.execute(
                "INSERT INTO projects (project_id, config_path, last_seen_at) VALUES (?1, ?2, ?3)",
                params![project_id, config_path, seen_at],
            )?;
        }
    }

    let db = Db::new_file(path.clone());
    db.ensure_schema()?;

    assert_eq!(user_version(&path)?, CURRENT_SCHEMA_VERSION);
    let projects = db.list_registered_projects()?;
    assert_eq!(projects.len(), 2);
    for project in &projects {
        assert!(!project.enabled, "{}: migrated enabled", project.project_id);
        assert_eq!(project.enabled_changed_at, None, "{}", project.project_id);
        assert_eq!(project.health, Default::default(), "{}", project.project_id);
        assert_eq!(project.last_seen_at.to_rfc3339(), seen_at);
    }
    assert_eq!(db.supervisor_record()?, Default::default());
    assert!(
        db.list_timeline_events(PROJECT, PAPER)?
            .iter()
            .all(|event| !event.event_type.starts_with("project_")),
        "migration must not write events"
    );

    for job in LEGACY_ROWS {
        assert_legacy_row_intact(&load_job(&db, job.id)?, job)?;
    }
    Ok(())
}

/// A pre-OSS-338 binary still upserts registrations with only the original
/// three columns; the defaults keep that working on a v6 database.
#[test]
fn old_registry_upserts_still_work_on_a_v6_database() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("reviewloop.db");
    let db = Db::new_file(path.clone());
    db.ensure_schema()?;
    Connection::open(&path)?.execute(
        r#"
        INSERT INTO projects (project_id, config_path, last_seen_at)
        VALUES (?1, ?2, ?3)
        ON CONFLICT(project_id) DO UPDATE SET
            config_path  = excluded.config_path,
            last_seen_at = excluded.last_seen_at
        "#,
        params![
            PROJECT,
            "/repos/legacy/reviewloop.toml",
            Utc::now().to_rfc3339()
        ],
    )?;
    let project = db
        .get_registered_project(PROJECT)?
        .context("old-style upsert registered nothing")?;
    assert!(!project.enabled);
    assert_eq!(project.enabled_changed_at, None);
    Ok(())
}

/// A pre-OSS-338 binary on a v6 database: its registry upsert and stale-row
/// delete become no-ops on an enabled project, so an explicit enable is
/// never moved or dropped behind the supervisor's back. Disabled rows behave
/// as that binary expects.
#[test]
fn old_binaries_cannot_move_or_drop_an_enabled_project() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("reviewloop.db");
    let db = Db::new_file(path.clone());
    db.ensure_schema()?;
    let now = Utc::now();
    let home = Path::new("/repos/enabled/reviewloop.toml");
    db.enable_project(PROJECT, home, None, now)?;
    db.insert_project_registration(
        OTHER_PROJECT,
        Path::new("/repos/other/reviewloop.toml"),
        now,
    )?;

    let old_upsert = |project_id: &str, config_path: &str| -> Result<()> {
        Connection::open(&path)?.execute(
            r#"
            INSERT INTO projects (project_id, config_path, last_seen_at)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(project_id) DO UPDATE SET
                config_path  = excluded.config_path,
                last_seen_at = excluded.last_seen_at
            "#,
            params![project_id, config_path, now.to_rfc3339()],
        )?;
        Ok(())
    };
    let old_delete = |project_id: &str| -> Result<()> {
        Connection::open(&path)?.execute(
            "DELETE FROM projects WHERE project_id = ?1",
            params![project_id],
        )?;
        Ok(())
    };

    old_upsert(PROJECT, "/repos/worktree/reviewloop.toml")?;
    old_delete(PROJECT)?;
    let enabled = db.get_registered_project(PROJECT)?.context("kept")?;
    assert!(enabled.enabled);
    assert_eq!(enabled.config_path, home);

    old_upsert(OTHER_PROJECT, "/repos/moved/reviewloop.toml")?;
    assert_eq!(
        db.resolve_project_config_path(OTHER_PROJECT)?,
        Some(PathBuf::from("/repos/moved/reviewloop.toml"))
    );
    old_delete(OTHER_PROJECT)?;
    assert!(db.get_registered_project(OTHER_PROJECT)?.is_none());

    // The supervisor's own decisions still move and release it.
    db.enable_project(
        PROJECT,
        Path::new("/repos/new/reviewloop.toml"),
        Some(home),
        now,
    )?;
    assert_eq!(
        db.resolve_project_config_path(PROJECT)?,
        Some(PathBuf::from("/repos/new/reviewloop.toml"))
    );
    Ok(())
}
