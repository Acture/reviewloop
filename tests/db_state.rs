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

        let mut config = Config {
            project_id: "project-db-state".to_string(),
            ..Config::default()
        };
        config.core.state_dir = state_dir.to_string_lossy().to_string();
        config.trigger.git.enabled = false;
        config.trigger.pdf.enabled = false;
        config.imap = None;
        config.providers.stanford.email = "test@example.edu".to_string();
        config.papers = vec![PaperConfig {
            id: "main".to_string(),
            pdf_path: pdf_path.to_string_lossy().to_string(),
            backend: "stanford".to_string(),
            venue: None,
        }];

        let db = Db::new(Path::new(&config.core.state_dir));
        db.ensure_schema()?;

        Ok(Self {
            _tmp: tmp,
            config,
            db,
        })
    }

    fn create_job_with_hash(&self, status: JobStatus, hash: &str) -> Result<Job> {
        let paper = &self.config.papers[0];
        self.db.create_job(&NewJob {
            project_id: self.config.project_id.clone(),
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

    fn create_job_with_project_and_hash(
        &self,
        project_id: &str,
        status: JobStatus,
        hash: &str,
    ) -> Result<Job> {
        let paper = &self.config.papers[0];
        self.db.create_job(&NewJob {
            project_id: project_id.to_string(),
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
}

#[test]
fn duplicate_guard_ignores_failed_jobs() -> Result<()> {
    let ctx = DbTestContext::new()?;

    ctx.create_job_with_hash(JobStatus::Failed, "same-hash")?;
    assert!(
        ctx.db
            .find_duplicate_covering_job(
                &ctx.config.project_id,
                "main",
                "stanford",
                "same-hash",
                "same-hash"
            )?
            .is_none()
    );

    ctx.create_job_with_hash(JobStatus::Queued, "same-hash")?;
    assert!(
        ctx.db
            .find_duplicate_covering_job(
                &ctx.config.project_id,
                "main",
                "stanford",
                "same-hash",
                "same-hash"
            )?
            .is_some()
    );

    Ok(())
}

#[test]
fn duplicate_guard_is_project_scoped() -> Result<()> {
    let ctx = DbTestContext::new()?;

    ctx.create_job_with_hash(JobStatus::Queued, "same-hash")?;
    assert!(
        ctx.db
            .find_duplicate_covering_job(
                "other-project",
                "main",
                "stanford",
                "same-hash",
                "same-hash"
            )?
            .is_none()
    );

    Ok(())
}

#[test]
fn version_and_round_progress_by_project_and_completed_rounds() -> Result<()> {
    let ctx = DbTestContext::new()?;
    let paper = &ctx.config.papers[0];

    let first = ctx.db.create_job(&NewJob {
        project_id: ctx.config.project_id.clone(),
        paper_id: paper.id.clone(),
        backend: paper.backend.clone(),
        pdf_path: paper.pdf_path.clone(),
        pdf_hash: "hash-v1".to_string(),
        status: JobStatus::Queued,
        email: ctx.config.providers.stanford.email.clone(),
        venue: ctx.config.providers.stanford.venue.clone(),
        git_tag: None,
        git_commit: None,
        next_poll_at: None,
    })?;
    assert_eq!(first.version_no, 1);
    assert_eq!(first.round_no, 1);
    assert_eq!(first.version_source, "pdf_hash");
    assert_eq!(first.version_key, "hash-v1");

    let parallel = ctx.db.create_job(&NewJob {
        project_id: ctx.config.project_id.clone(),
        paper_id: paper.id.clone(),
        backend: paper.backend.clone(),
        pdf_path: paper.pdf_path.clone(),
        pdf_hash: "hash-v1".to_string(),
        status: JobStatus::Queued,
        email: ctx.config.providers.stanford.email.clone(),
        venue: ctx.config.providers.stanford.venue.clone(),
        git_tag: None,
        git_commit: None,
        next_poll_at: None,
    })?;
    assert_eq!(parallel.version_no, 1);
    assert_eq!(parallel.round_no, 1);

    // Shortcutting to Completed directly from Queued (test helper only;
    // the real worker goes Queued -> Processing -> Completed).
    ctx.db.update_job_state_unchecked(
        &first.id,
        JobStatus::Completed,
        None,
        Some(None),
        Some(None),
    )?;

    let next_round = ctx.db.create_job(&NewJob {
        project_id: ctx.config.project_id.clone(),
        paper_id: paper.id.clone(),
        backend: paper.backend.clone(),
        pdf_path: paper.pdf_path.clone(),
        pdf_hash: "hash-v1".to_string(),
        status: JobStatus::Queued,
        email: ctx.config.providers.stanford.email.clone(),
        venue: ctx.config.providers.stanford.venue.clone(),
        git_tag: None,
        git_commit: None,
        next_poll_at: None,
    })?;
    assert_eq!(next_round.version_no, 1);
    assert_eq!(next_round.round_no, 2);

    let new_version = ctx.db.create_job(&NewJob {
        project_id: ctx.config.project_id.clone(),
        paper_id: paper.id.clone(),
        backend: paper.backend.clone(),
        pdf_path: paper.pdf_path.clone(),
        pdf_hash: "hash-v2".to_string(),
        status: JobStatus::Queued,
        email: ctx.config.providers.stanford.email.clone(),
        venue: ctx.config.providers.stanford.venue.clone(),
        git_tag: None,
        git_commit: Some("commit-v2".to_string()),
        next_poll_at: None,
    })?;
    assert_eq!(new_version.version_no, 2);
    assert_eq!(new_version.round_no, 1);
    assert_eq!(new_version.version_source, "git_commit");
    assert_eq!(new_version.version_key, "commit-v2");

    let same_version = ctx.db.create_job(&NewJob {
        project_id: ctx.config.project_id.clone(),
        paper_id: paper.id.clone(),
        backend: paper.backend.clone(),
        pdf_path: paper.pdf_path.clone(),
        pdf_hash: "hash-v3".to_string(),
        status: JobStatus::Queued,
        email: ctx.config.providers.stanford.email.clone(),
        venue: ctx.config.providers.stanford.venue.clone(),
        git_tag: None,
        git_commit: Some("commit-v2".to_string()),
        next_poll_at: None,
    })?;
    assert_eq!(same_version.version_no, 2);
    assert_eq!(same_version.round_no, 1);

    let other_project = ctx.db.create_job(&NewJob {
        project_id: "other-project".to_string(),
        paper_id: paper.id.clone(),
        backend: paper.backend.clone(),
        pdf_path: paper.pdf_path.clone(),
        pdf_hash: "hash-v1".to_string(),
        status: JobStatus::Queued,
        email: ctx.config.providers.stanford.email.clone(),
        venue: ctx.config.providers.stanford.venue.clone(),
        git_tag: None,
        git_commit: None,
        next_poll_at: None,
    })?;
    assert_eq!(other_project.version_no, 1);
    assert_eq!(other_project.round_no, 1);

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

    let jobs = ctx
        .db
        .list_ready_queued(&ctx.config.project_id, 10, Utc::now())?;
    let ids = jobs.into_iter().map(|j| j.id).collect::<Vec<_>>();
    assert!(ids.contains(&a.id));
    assert!(!ids.contains(&b.id));

    Ok(())
}

#[test]
fn list_due_processing_treats_past_next_poll_as_due() -> Result<()> {
    let ctx = DbTestContext::new()?;
    let job = ctx.create_job(JobStatus::Queued)?;
    let past = Utc::now() - Duration::minutes(5);
    ctx.db.attach_token_to_job(&job.id, "tok-past-due", past)?;

    let due = ctx
        .db
        .list_due_processing(&ctx.config.project_id, 10, Utc::now())?;
    assert!(due.iter().any(|j| j.id == job.id));
    Ok(())
}

#[test]
fn purge_paper_history_removes_jobs_events_reviews_for_target_paper() -> Result<()> {
    let ctx = DbTestContext::new()?;
    let main_job = ctx.create_job(JobStatus::Queued)?;
    ctx.db
        .attach_token_to_job(&main_job.id, "tok-main-purge", Utc::now())?;
    ctx.db.add_event(
        Some(&ctx.config.project_id),
        Some(&main_job.id),
        "main_event",
        json!({"scope":"main"}),
    )?;
    ctx.db
        .upsert_review(&main_job.id, "tok-main-purge", r#"{"ok":true}"#, "summary")?;

    let other_job = ctx.db.create_job(&NewJob {
        project_id: ctx.config.project_id.clone(),
        paper_id: "other".to_string(),
        backend: "stanford".to_string(),
        pdf_path: ctx.config.papers[0].pdf_path.clone(),
        pdf_hash: "other-hash".to_string(),
        status: JobStatus::Queued,
        email: ctx.config.providers.stanford.email.clone(),
        venue: ctx.config.providers.stanford.venue.clone(),
        git_tag: None,
        git_commit: None,
        next_poll_at: None,
    })?;
    ctx.db.add_event(
        Some(&ctx.config.project_id),
        Some(&other_job.id),
        "other_event",
        json!({"scope":"other"}),
    )?;

    let report = ctx.db.purge_paper_history(&ctx.config.project_id, "main")?;
    assert_eq!(report.jobs, 1);
    assert_eq!(report.reviews, 1);
    assert!(report.events >= 1);
    assert!(report.job_ids.iter().any(|id| id == &main_job.id));

    assert!(ctx.db.get_job(&main_job.id)?.is_none());
    assert!(ctx.db.get_job(&other_job.id)?.is_some());

    let conn = rusqlite::Connection::open(&ctx.db.path)?;
    let main_reviews: i64 = conn.query_row(
        "SELECT COUNT(*) FROM reviews WHERE job_id = ?1",
        params![main_job.id],
        |row| row.get(0),
    )?;
    assert_eq!(main_reviews, 0);

    let main_events: i64 = conn.query_row(
        "SELECT COUNT(*) FROM events WHERE job_id = ?1",
        params![main_job.id],
        |row| row.get(0),
    )?;
    assert_eq!(main_events, 0);

    let other_events: i64 = conn.query_row(
        "SELECT COUNT(*) FROM events WHERE job_id = ?1",
        params![other_job.id],
        |row| row.get(0),
    )?;
    assert_eq!(other_events, 1);

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
        .find_latest_open_job_without_token(&ctx.config.project_id, "stanford")?
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
        "UPDATE jobs SET started_at = ?1, created_at = ?1, updated_at = ?1 WHERE id = ?2",
        params![old, job.id],
    )?;

    worker::mark_timeouts(&ctx.config, &ctx.db)?;

    let updated = ctx.db.get_job(&job.id)?.context("missing timed out job")?;
    assert_eq!(updated.status, JobStatus::Timeout);

    Ok(())
}

#[test]
fn mark_timeouts_scales_with_pdf_pages_for_stanford() -> Result<()> {
    let ctx = DbTestContext::new()?;
    let mut synthetic_pdf = String::from("%PDF-1.4\n");
    for _ in 0..10 {
        synthetic_pdf.push_str("<< /Type /Page >>\n");
    }
    synthetic_pdf.push_str("%%EOF\n");
    fs::write(&ctx.config.papers[0].pdf_path, synthetic_pdf.as_bytes())?;

    let job = ctx.create_job(JobStatus::Queued)?;
    ctx.db
        .attach_token_to_job(&job.id, "token-scale", Utc::now() - Duration::minutes(1))?;

    let conn = rusqlite::Connection::open(&ctx.db.path)?;
    let started = (Utc::now() - Duration::hours(25)).to_rfc3339();
    conn.execute(
        "UPDATE jobs SET started_at = ?1, created_at = ?1, updated_at = ?1 WHERE id = ?2",
        params![started, job.id],
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
        .find_job_by_token(&ctx.config.project_id, "tok-by-token")?
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

    let mut config = Config {
        project_id: "project-db-state".to_string(),
        ..Config::default()
    };
    config.core.state_dir = state_dir.to_string_lossy().to_string();
    config.core.db_path = ":memory:".to_string();
    config.providers.stanford.email = "test@example.edu".to_string();
    config.papers = vec![PaperConfig {
        id: "main".to_string(),
        pdf_path: pdf_path.to_string_lossy().to_string(),
        backend: "stanford".to_string(),
        venue: None,
    }];

    let db = Db::from_config(&config)?;
    db.ensure_schema()?;

    let hash = sha256_file(Path::new(&config.papers[0].pdf_path))?;
    let created = db.create_job(&NewJob {
        project_id: config.project_id.clone(),
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
    ctx.db.add_event(
        Some(&ctx.config.project_id),
        None,
        "test_event",
        json!({"kind":"stale"}),
    )?;

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
        Some(&ctx.config.project_id),
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

// ── U2: list_failed_jobs_for_project ─────────────────────────────────────────

#[test]
fn list_failed_jobs_for_project_returns_terminal_statuses_only() -> Result<()> {
    let ctx = DbTestContext::new()?;

    // Create one job of each terminal-failure status and some non-failure ones.
    let failed = ctx.create_job_with_hash(JobStatus::Failed, "hash-f1")?;
    let needs_manual = ctx.create_job_with_hash(JobStatus::FailedNeedsManual, "hash-f2")?;
    let timeout = ctx.create_job_with_hash(JobStatus::Timeout, "hash-f3")?;
    let _completed = ctx.create_job_with_hash(JobStatus::Completed, "hash-c1")?;
    let _queued = ctx.create_job_with_hash(JobStatus::Queued, "hash-q1")?;

    let results = ctx
        .db
        .list_failed_jobs_for_project(&ctx.config.project_id, 20)?;

    assert_eq!(results.len(), 3, "expected exactly 3 failed jobs");

    let ids: std::collections::HashSet<_> = results.iter().map(|j| j.id.as_str()).collect();
    assert!(ids.contains(failed.id.as_str()));
    assert!(ids.contains(needs_manual.id.as_str()));
    assert!(ids.contains(timeout.id.as_str()));

    // Must not include Completed or Queued.
    for job in &results {
        assert!(
            matches!(
                job.status,
                JobStatus::Failed | JobStatus::FailedNeedsManual | JobStatus::Timeout
            ),
            "unexpected status {:?} in failed list",
            job.status
        );
    }

    Ok(())
}

#[test]
fn list_failed_jobs_for_project_orders_by_updated_at_desc() -> Result<()> {
    let ctx = DbTestContext::new()?;

    let j1 = ctx.create_job_with_hash(JobStatus::Failed, "hash-o1")?;
    let j2 = ctx.create_job_with_hash(JobStatus::Failed, "hash-o2")?;
    let j3 = ctx.create_job_with_hash(JobStatus::Failed, "hash-o3")?;

    // Manually set updated_at so we have a predictable order.
    let conn = rusqlite::Connection::open(&ctx.db.path)?;
    let now = Utc::now();
    conn.execute(
        "UPDATE jobs SET updated_at = ?1 WHERE id = ?2",
        params![(now - Duration::seconds(20)).to_rfc3339(), j1.id],
    )?;
    conn.execute(
        "UPDATE jobs SET updated_at = ?1 WHERE id = ?2",
        params![(now - Duration::seconds(10)).to_rfc3339(), j2.id],
    )?;
    conn.execute(
        "UPDATE jobs SET updated_at = ?1 WHERE id = ?2",
        params![now.to_rfc3339(), j3.id],
    )?;

    let results = ctx
        .db
        .list_failed_jobs_for_project(&ctx.config.project_id, 20)?;

    assert_eq!(results.len(), 3);
    assert_eq!(results[0].id, j3.id, "most recent should be first");
    assert_eq!(results[1].id, j2.id);
    assert_eq!(results[2].id, j1.id);

    Ok(())
}

#[test]
fn list_failed_jobs_for_project_respects_limit() -> Result<()> {
    let ctx = DbTestContext::new()?;

    for i in 0..5 {
        ctx.create_job_with_hash(JobStatus::Failed, &format!("hash-lim-{i}"))?;
    }

    let results = ctx
        .db
        .list_failed_jobs_for_project(&ctx.config.project_id, 3)?;

    assert_eq!(results.len(), 3, "limit=3 should cap at 3 results");

    Ok(())
}

#[test]
fn list_active_jobs_all_returns_jobs_from_every_project() -> Result<()> {
    let ctx = DbTestContext::new()?;

    let a = ctx.create_job_with_project_and_hash("proj-a", JobStatus::Queued, "ha-1")?;
    let b = ctx.create_job_with_project_and_hash("proj-b", JobStatus::Submitted, "hb-1")?;
    let c = ctx.create_job_with_project_and_hash("proj-c", JobStatus::Processing, "hc-1")?;
    // A completed job from another project should NOT appear (not active).
    let _completed =
        ctx.create_job_with_project_and_hash("proj-d", JobStatus::Completed, "hd-1")?;

    let results = ctx.db.list_active_jobs_all()?;
    let ids: std::collections::HashSet<_> = results.iter().map(|j| j.id.as_str()).collect();
    assert_eq!(results.len(), 3, "expected 3 active jobs across 3 projects");
    assert!(ids.contains(a.id.as_str()));
    assert!(ids.contains(b.id.as_str()));
    assert!(ids.contains(c.id.as_str()));

    // project_id field is preserved so the bar can group by it.
    let projects: std::collections::HashSet<_> =
        results.iter().map(|j| j.project_id.as_str()).collect();
    assert!(projects.contains("proj-a"));
    assert!(projects.contains("proj-b"));
    assert!(projects.contains("proj-c"));

    Ok(())
}

#[test]
fn list_failed_jobs_all_per_project_caps_per_project_not_globally() -> Result<()> {
    let ctx = DbTestContext::new()?;

    // Project A: 8 failures (noisy).
    for i in 0..8 {
        ctx.create_job_with_project_and_hash("noisy", JobStatus::Failed, &format!("noisy-{i}"))?;
    }
    // Project B: 1 failure.
    ctx.create_job_with_project_and_hash("quiet", JobStatus::Failed, "quiet-1")?;

    // Per-project limit = 3 should yield 3 from "noisy" + 1 from "quiet" = 4.
    // A naive global LIMIT 3 would have hidden "quiet" entirely.
    let results = ctx.db.list_failed_jobs_all_per_project(3)?;
    let by_project: std::collections::HashMap<&str, usize> =
        results
            .iter()
            .fold(std::collections::HashMap::new(), |mut acc, j| {
                *acc.entry(j.project_id.as_str()).or_insert(0) += 1;
                acc
            });

    assert_eq!(by_project.get("noisy").copied().unwrap_or(0), 3);
    assert_eq!(by_project.get("quiet").copied().unwrap_or(0), 1);
    assert_eq!(results.len(), 4);

    Ok(())
}

#[test]
fn list_failed_jobs_all_per_project_excludes_user_cancellations() -> Result<()> {
    let ctx = DbTestContext::new()?;

    let cancelled = ctx.create_job_with_project_and_hash("p", JobStatus::Failed, "cancel-1")?;
    let cancelled_with_reason =
        ctx.create_job_with_project_and_hash("p", JobStatus::Failed, "cancel-2")?;
    let real_failure = ctx.create_job_with_project_and_hash("p", JobStatus::Failed, "real-1")?;

    // Mark two as user-cancelled: one with the default message and one with a reason.
    let conn = rusqlite::Connection::open(&ctx.db.path)?;
    conn.execute(
        "UPDATE jobs SET last_error = ?1 WHERE id = ?2",
        params!["cancelled by user", cancelled.id],
    )?;
    conn.execute(
        "UPDATE jobs SET last_error = ?1 WHERE id = ?2",
        params![
            "cancelled by user: requested via reviewloop cancel",
            cancelled_with_reason.id
        ],
    )?;

    let results = ctx.db.list_failed_jobs_all_per_project(10)?;
    let ids: std::collections::HashSet<_> = results.iter().map(|j| j.id.as_str()).collect();
    assert!(!ids.contains(cancelled.id.as_str()));
    assert!(!ids.contains(cancelled_with_reason.id.as_str()));
    assert!(ids.contains(real_failure.id.as_str()));

    Ok(())
}
