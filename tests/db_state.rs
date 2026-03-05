use anyhow::{Context, Result};
use chrono::{Duration, Utc};
use reviewloop::{
    config::{Config, PaperConfig},
    db::Db,
    model::{Job, JobStatus, NewJob},
    util::sha256_file,
    worker,
};
use rusqlite::params;
use serde_json::json;
use std::{fs, path::Path};

struct DbTestContext {
    _tmp: tempfile::TempDir,
    config: Config,
    db: Db,
}

impl DbTestContext {
    fn new() -> Result<Self> {
        let tmp = tempfile::tempdir()?;
        let state_dir = tmp.path().join("state");
        fs::create_dir_all(&state_dir)?;

        let pdf_path = tmp.path().join("paper.pdf");
        fs::write(&pdf_path, b"%PDF-1.4\n%%EOF\n")?;

        let mut config = Config::default();
        config.core.state_dir = state_dir.to_string_lossy().to_string();
        config.trigger.git.enabled = false;
        config.trigger.pdf.enabled = false;
        config.imap = None;
        config.providers.stanford.email = "test@example.edu".to_string();
        config.papers = vec![PaperConfig {
            id: "main".to_string(),
            pdf_path: pdf_path.to_string_lossy().to_string(),
            backend: "stanford".to_string(),
        }];

        let db = Db::new(Path::new(&config.core.state_dir));
        db.init_schema()?;

        Ok(Self {
            _tmp: tmp,
            config,
            db,
        })
    }

    fn create_job_with_hash(&self, status: JobStatus, hash: &str) -> Result<Job> {
        let paper = &self.config.papers[0];
        self.db.create_job(&NewJob {
            paper_id: paper.id.clone(),
            backend: paper.backend.clone(),
            pdf_path: paper.pdf_path.clone(),
            pdf_hash: hash.to_string(),
            status,
            email: self.config.providers.stanford.email.clone(),
            venue: self.config.providers.stanford.venue.clone(),
            git_tag: None,
            git_commit: None,
            next_poll_at: None,
        })
    }

    fn create_job(&self, status: JobStatus) -> Result<Job> {
        let paper = &self.config.papers[0];
        let hash = sha256_file(Path::new(&paper.pdf_path))?;
        self.create_job_with_hash(status, &hash)
    }
}

#[test]
fn duplicate_guard_ignores_failed_jobs() -> Result<()> {
    let ctx = DbTestContext::new()?;

    ctx.create_job_with_hash(JobStatus::Failed, "same-hash")?;
    assert!(!ctx.db.has_duplicate_guard("stanford", "same-hash")?);

    ctx.create_job_with_hash(JobStatus::Queued, "same-hash")?;
    assert!(ctx.db.has_duplicate_guard("stanford", "same-hash")?);

    Ok(())
}

#[test]
fn list_ready_queued_respects_next_poll_at() -> Result<()> {
    let ctx = DbTestContext::new()?;
    let a = ctx.create_job(JobStatus::Queued)?;

    let b_hash = "future-hash";
    let b = ctx.create_job_with_hash(JobStatus::Queued, b_hash)?;
    let future = Utc::now() + Duration::hours(2);
    ctx.db.update_job_state(
        &b.id,
        JobStatus::Queued,
        Some(0),
        Some(Some(future)),
        Some(None),
    )?;

    let jobs = ctx.db.list_ready_queued(10, Utc::now())?;
    let ids = jobs.into_iter().map(|j| j.id).collect::<Vec<_>>();
    assert!(ids.contains(&a.id));
    assert!(!ids.contains(&b.id));

    Ok(())
}

#[test]
fn latest_open_job_without_token_prefers_newest() -> Result<()> {
    let ctx = DbTestContext::new()?;
    let older = ctx.create_job_with_hash(JobStatus::Queued, "hash-1")?;

    std::thread::sleep(std::time::Duration::from_millis(2));
    let newer = ctx.create_job_with_hash(JobStatus::Submitted, "hash-2")?;

    let got = ctx
        .db
        .find_latest_open_job_without_token("stanford")?
        .context("expected an open job")?;

    assert_ne!(older.id, newer.id);
    assert_eq!(got.id, newer.id);
    Ok(())
}

#[test]
fn attach_token_moves_job_to_processing() -> Result<()> {
    let ctx = DbTestContext::new()?;
    let job = ctx.create_job(JobStatus::Queued)?;

    ctx.db
        .attach_token_to_job(&job.id, "token-xyz", Utc::now() + Duration::minutes(10))?;

    let updated = ctx
        .db
        .get_job(&job.id)?
        .context("missing job after attach")?;
    assert_eq!(updated.status, JobStatus::Processing);
    assert_eq!(updated.token.as_deref(), Some("token-xyz"));
    assert!(updated.started_at.is_some());

    Ok(())
}

#[test]
fn mark_timeouts_moves_old_processing_jobs_to_timeout() -> Result<()> {
    let ctx = DbTestContext::new()?;
    let job = ctx.create_job(JobStatus::Queued)?;
    ctx.db
        .attach_token_to_job(&job.id, "token-old", Utc::now() - Duration::minutes(1))?;

    let conn = rusqlite::Connection::open(&ctx.db.path)?;
    let old = (Utc::now() - Duration::hours(49)).to_rfc3339();
    conn.execute(
        "UPDATE jobs SET created_at = ?1, updated_at = ?1 WHERE id = ?2",
        params![old, job.id],
    )?;

    worker::mark_timeouts(&ctx.config, &ctx.db)?;

    let updated = ctx.db.get_job(&job.id)?.context("missing timed out job")?;
    assert_eq!(updated.status, JobStatus::Timeout);

    Ok(())
}

#[test]
fn find_job_by_token_returns_bound_job() -> Result<()> {
    let ctx = DbTestContext::new()?;
    let job = ctx.create_job(JobStatus::Queued)?;
    ctx.db
        .attach_token_to_job(&job.id, "tok-by-token", Utc::now())?;

    let found = ctx
        .db
        .find_job_by_token("tok-by-token")?
        .context("expected token-bound job")?;
    assert_eq!(found.id, job.id);
    assert_eq!(found.token.as_deref(), Some("tok-by-token"));
    Ok(())
}

#[test]
fn in_memory_db_persists_across_operations_for_same_instance() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let state_dir = tmp.path().join("state");
    fs::create_dir_all(&state_dir)?;

    let pdf_path = tmp.path().join("paper.pdf");
    fs::write(&pdf_path, b"%PDF-1.4\n%%EOF\n")?;

    let mut config = Config::default();
    config.core.state_dir = state_dir.to_string_lossy().to_string();
    config.core.db_path = ":memory:".to_string();
    config.providers.stanford.email = "test@example.edu".to_string();
    config.papers = vec![PaperConfig {
        id: "main".to_string(),
        pdf_path: pdf_path.to_string_lossy().to_string(),
        backend: "stanford".to_string(),
    }];

    let db = Db::from_config(&config)?;
    db.init_schema()?;

    let hash = sha256_file(Path::new(&config.papers[0].pdf_path))?;
    let created = db.create_job(&NewJob {
        paper_id: "main".to_string(),
        backend: "stanford".to_string(),
        pdf_path: config.papers[0].pdf_path.clone(),
        pdf_hash: hash,
        status: JobStatus::Queued,
        email: config.providers.stanford.email.clone(),
        venue: None,
        git_tag: None,
        git_commit: None,
        next_poll_at: None,
    })?;

    let loaded = db
        .get_job(&created.id)?
        .context("missing job from in-memory sqlite")?;
    assert_eq!(loaded.id, created.id);
    assert_eq!(loaded.status, JobStatus::Queued);

    Ok(())
}

#[test]
fn retention_prunes_stale_auxiliary_entries() -> Result<()> {
    let mut ctx = DbTestContext::new()?;
    let now = Utc::now();

    ctx.db
        .record_email_token("tok-old", "imap:stanford", Some("ref"))?;
    ctx.db.mark_tag_seen("review-stanford/main/v1", "abc123")?;
    ctx.db
        .add_event(None, "test_event", json!({"kind":"stale"}))?;

    let conn = rusqlite::Connection::open(&ctx.db.path)?;
    let old = (now - Duration::days(120)).to_rfc3339();
    conn.execute(
        "UPDATE email_tokens SET matched_at = ?1 WHERE token = ?2",
        params![old, "tok-old"],
    )?;
    conn.execute(
        "UPDATE seen_tags SET seen_at = ?1 WHERE tag_name = ?2",
        params![old, "review-stanford/main/v1"],
    )?;
    conn.execute("UPDATE events SET created_at = ?1", params![old])?;

    ctx.config.retention.email_tokens_days = 30;
    ctx.config.retention.seen_tags_days = 30;
    ctx.config.retention.events_days = 30;
    ctx.config.retention.terminal_jobs_days = 0;

    let report = ctx.db.prune_retention(&ctx.config.retention, now)?;
    assert_eq!(report.email_tokens, 1);
    assert_eq!(report.seen_tags, 1);
    assert_eq!(report.events, 1);
    assert_eq!(report.jobs, 0);
    assert_eq!(report.reviews, 0);

    let remaining_tokens: i64 =
        conn.query_row("SELECT COUNT(*) FROM email_tokens", [], |row| row.get(0))?;
    let remaining_tags: i64 =
        conn.query_row("SELECT COUNT(*) FROM seen_tags", [], |row| row.get(0))?;
    let remaining_events: i64 =
        conn.query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))?;
    assert_eq!(remaining_tokens, 0);
    assert_eq!(remaining_tags, 0);
    assert_eq!(remaining_events, 0);

    Ok(())
}

#[test]
fn retention_prunes_old_terminal_jobs_when_enabled() -> Result<()> {
    let mut ctx = DbTestContext::new()?;
    let now = Utc::now();

    let completed = ctx.create_job_with_hash(JobStatus::Completed, "hash-completed")?;
    let processing = ctx.create_job_with_hash(JobStatus::Processing, "hash-processing")?;
    ctx.db
        .upsert_review(&completed.id, "tok-completed", r#"{"ok":true}"#, "summary")?;
    ctx.db.add_event(
        Some(&completed.id),
        "completed_event",
        json!({"job":"completed"}),
    )?;

    let conn = rusqlite::Connection::open(&ctx.db.path)?;
    let old = (now - Duration::days(10)).to_rfc3339();
    conn.execute(
        "UPDATE jobs SET updated_at = ?1 WHERE id = ?2",
        params![old, completed.id],
    )?;
    conn.execute(
        "UPDATE jobs SET updated_at = ?1 WHERE id = ?2",
        params![old, processing.id],
    )?;

    ctx.config.retention.email_tokens_days = 0;
    ctx.config.retention.seen_tags_days = 0;
    ctx.config.retention.events_days = 0;
    ctx.config.retention.terminal_jobs_days = 7;

    let report = ctx.db.prune_retention(&ctx.config.retention, now)?;
    assert_eq!(report.jobs, 1);
    assert_eq!(report.reviews, 1);
    assert_eq!(report.events, 1);

    assert!(ctx.db.get_job(&completed.id)?.is_none());
    assert!(ctx.db.get_job(&processing.id)?.is_some());

    let review_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM reviews WHERE job_id = ?1",
        params![completed.id],
        |row| row.get(0),
    )?;
    assert_eq!(review_count, 0);

    Ok(())
}
