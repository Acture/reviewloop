use crate::{
    artifact::write_review_artifacts,
    backend::{BackendError, ReviewFetchResult, SubmitRequest, build_backend},
    config::Config,
    db::Db,
    email::poll_imap_if_enabled,
    fallback::submit_with_node_playwright,
    model::{Job, JobStatus},
    panel::render_tick_panel,
    trigger::{run_git_tag_trigger, run_pdf_trigger},
    util::compute_next_poll_at,
};
use anyhow::{Context, Result};
use chrono::{Duration, Utc};
use serde_json::json;
use std::{path::Path, time::Duration as StdDuration};
use tracing::{error, info, warn};

const RATE_LIMIT_COOLDOWN_MINUTES: i64 = 30;
const TERMINAL_REVIEW_FAILURE_HINTS: [&str; 3] = [
    "review generation failed",
    "unable to generate review",
    "failed to generate review",
];

fn is_terminal_review_generation_failure(body: &str) -> bool {
    let normalized = body.to_ascii_lowercase();
    let has_failure_hint = TERMINAL_REVIEW_FAILURE_HINTS
        .iter()
        .any(|hint| normalized.contains(hint));

    has_failure_hint && normalized.contains("contact support")
}

pub async fn run_daemon(config: &Config, db: &Db, panel: bool) -> Result<()> {
    info!("daemon started");
    let mut tick: u64 = 0;
    loop {
        tick += 1;
        let mut last_tick_error: Option<String> = None;

        if let Err(err) = run_tick_internal(config, db, Some(tick)).await {
            let msg = format!("{err:#}");
            error!(tick, error = %msg, "tick failed");
            last_tick_error = Some(msg);
        }

        if panel {
            render_tick_panel(config, db, tick, last_tick_error.as_deref())?;
        }

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("received Ctrl+C, daemon exiting");
                break;
            }
            _ = tokio::time::sleep(StdDuration::from_secs(30)) => {}
        }
    }

    info!("daemon stopped");
    Ok(())
}

pub async fn run_tick(config: &Config, db: &Db) -> Result<()> {
    run_tick_internal(config, db, None).await
}

async fn run_tick_internal(config: &Config, db: &Db, tick: Option<u64>) -> Result<()> {
    run_git_tag_trigger(config, db)?;
    run_pdf_trigger(config, db)?;

    poll_imap_if_enabled(config, db).await?;

    mark_timeouts(config, db)?;
    process_submissions(config, db).await?;
    process_polls(config, db).await?;
    prune_retention(config, db, tick)?;

    Ok(())
}

pub async fn process_submissions(config: &Config, db: &Db) -> Result<()> {
    let per_tick_budget = usize::min(
        config.core.max_concurrency,
        config.core.max_submissions_per_tick,
    );
    let jobs = db.list_ready_queued(per_tick_budget, Utc::now())?;
    for job in jobs {
        submit_job(config, db, &job.id).await?;
    }

    Ok(())
}

pub async fn process_polls(config: &Config, db: &Db) -> Result<()> {
    let jobs = db.list_due_processing(config.core.max_concurrency, Utc::now())?;
    for job in jobs {
        poll_job(config, db, &job).await?;
    }
    Ok(())
}

pub async fn submit_job(config: &Config, db: &Db, job_id: &str) -> Result<()> {
    let Some(job) = db.get_job(job_id)? else {
        anyhow::bail!("job not found: {job_id}");
    };

    let paper = config
        .find_paper(&job.paper_id)
        .with_context(|| format!("paper not found in config: {}", job.paper_id))?;

    let backend = build_backend(config, &job.backend)?;

    let (email, venue) = match job.backend.as_str() {
        "stanford" => {
            let email = if job.email.trim().is_empty() {
                config.providers.stanford.email.clone()
            } else {
                job.email.clone()
            };
            let venue = job
                .venue
                .clone()
                .or_else(|| config.providers.stanford.venue.clone());
            (email, venue)
        }
        _ => (job.email.clone(), job.venue.clone()),
    };

    db.update_job_state(
        &job.id,
        JobStatus::Submitted,
        Some(job.attempt),
        Some(None),
        Some(None),
    )?;

    let submit_req = SubmitRequest {
        pdf_path: Path::new(&paper.pdf_path).to_path_buf(),
        email,
        venue,
    };

    match backend.submit(submit_req).await {
        Ok(receipt) => {
            let _ = &receipt.backend_submission_ref;
            let next_poll = compute_next_poll_at(
                Utc::now(),
                &config.polling.schedule_minutes,
                0,
                config.polling.jitter_percent,
            );
            db.mark_submitted_with_token(&job.id, &receipt.token, next_poll)?;
            db.add_event(
                Some(&job.id),
                "submitted",
                json!({ "backend": backend.name(), "token": receipt.token }),
            )?;
            info!(job_id = %job.id, backend = %backend.name(), "job submitted");
            Ok(())
        }
        Err(BackendError::RateLimited(message)) => {
            let next = Utc::now() + Duration::minutes(RATE_LIMIT_COOLDOWN_MINUTES);
            db.update_job_state(
                &job.id,
                JobStatus::Queued,
                Some(job.attempt + 1),
                Some(Some(next)),
                Some(Some(message.clone())),
            )?;
            db.add_event(
                Some(&job.id),
                "submit_rate_limited",
                json!({ "message": message, "cooldown_minutes": RATE_LIMIT_COOLDOWN_MINUTES }),
            )?;
            warn!(job_id = %job.id, "submit rate limited; cooldown applied");
            Ok(())
        }
        Err(err) => handle_submit_error_with_fallback(config, db, &job, err).await,
    }
}

async fn handle_submit_error_with_fallback(
    config: &Config,
    db: &Db,
    job: &Job,
    err: BackendError,
) -> Result<()> {
    let can_fallback = job.backend == "stanford"
        && !job.fallback_used
        && config.providers.stanford.fallback_mode == "node_playwright";

    if can_fallback {
        let email = if job.email.trim().is_empty() {
            config.providers.stanford.email.clone()
        } else {
            job.email.clone()
        };

        match submit_with_node_playwright(
            Path::new(&config.providers.stanford.fallback_script),
            &config.providers.stanford.base_url,
            Path::new(&job.pdf_path),
            &email,
            job.venue
                .as_deref()
                .or(config.providers.stanford.venue.as_deref()),
        )
        .await
        {
            Ok(receipt) => {
                let next_poll = compute_next_poll_at(
                    Utc::now(),
                    &config.polling.schedule_minutes,
                    0,
                    config.polling.jitter_percent,
                );
                db.mark_fallback_used(&job.id)?;
                db.mark_submitted_with_token(&job.id, &receipt.token, next_poll)?;
                db.add_event(
                    Some(&job.id),
                    "submitted_via_fallback",
                    json!({ "token": receipt.token }),
                )?;
                warn!(job_id = %job.id, "job submitted via fallback script");
                return Ok(());
            }
            Err(fallback_err) => {
                let reason = format!("primary submit error: {err}; fallback error: {fallback_err}");
                db.update_job_state(
                    &job.id,
                    JobStatus::FailedNeedsManual,
                    Some(job.attempt + 1),
                    Some(None),
                    Some(Some(reason.clone())),
                )?;
                db.add_event(
                    Some(&job.id),
                    "submit_failed_needs_manual",
                    json!({ "reason": reason }),
                )?;
                error!(job_id = %job.id, "submit failed and fallback failed; manual intervention required");
                return Ok(());
            }
        }
    }

    let reason = err.to_string();
    db.update_job_state(
        &job.id,
        JobStatus::Failed,
        Some(job.attempt + 1),
        Some(None),
        Some(Some(reason.clone())),
    )?;
    db.add_event(Some(&job.id), "submit_failed", json!({ "reason": reason }))?;
    error!(job_id = %job.id, "submit failed");
    Ok(())
}

pub async fn poll_job(config: &Config, db: &Db, job: &Job) -> Result<()> {
    let token = job
        .token
        .as_deref()
        .with_context(|| format!("job {} has no token", job.id))?;

    let backend = build_backend(config, &job.backend)?;

    match backend.fetch_review(token).await {
        Ok(ReviewFetchResult::Processing) => {
            let next = compute_next_poll_at(
                Utc::now(),
                &config.polling.schedule_minutes,
                job.attempt + 1,
                config.polling.jitter_percent,
            );
            db.update_job_state(
                &job.id,
                JobStatus::Processing,
                Some(job.attempt + 1),
                Some(Some(next)),
                Some(None),
            )?;
            db.add_event(
                Some(&job.id),
                "poll_processing",
                json!({ "attempt": job.attempt + 1, "next_poll_at": next.to_rfc3339() }),
            )?;
        }
        Ok(ReviewFetchResult::Ready { raw_json }) => {
            let (_, summary_md, _) =
                write_review_artifacts(&config.state_dir(), job, token, &raw_json)?;
            db.upsert_review(&job.id, token, &raw_json.to_string(), &summary_md)?;
            db.update_job_state(
                &job.id,
                JobStatus::Completed,
                Some(job.attempt + 1),
                Some(None),
                Some(None),
            )?;
            db.add_event(Some(&job.id), "review_completed", json!({ "token": token }))?;
            info!(job_id = %job.id, "review completed and artifacts written");
        }
        Ok(ReviewFetchResult::InvalidToken) => {
            db.update_job_state(
                &job.id,
                JobStatus::Failed,
                Some(job.attempt + 1),
                Some(None),
                Some(Some("invalid token".to_string())),
            )?;
            db.add_event(Some(&job.id), "invalid_token", json!({ "token": token }))?;
            warn!(job_id = %job.id, "invalid token reported by backend");
        }
        Err(BackendError::RateLimited(message)) => {
            let next = Utc::now() + Duration::minutes(RATE_LIMIT_COOLDOWN_MINUTES);
            db.update_job_state(
                &job.id,
                JobStatus::Processing,
                Some(job.attempt + 1),
                Some(Some(next)),
                Some(Some(message.clone())),
            )?;
            db.add_event(
                Some(&job.id),
                "poll_rate_limited",
                json!({ "message": message, "next_poll_at": next.to_rfc3339() }),
            )?;
            warn!(job_id = %job.id, "poll rate limited; cooldown applied");
        }
        Err(BackendError::Server { status, body }) => {
            if is_terminal_review_generation_failure(&body) {
                let reason = format!("terminal backend error ({status}): {body}");
                db.update_job_state(
                    &job.id,
                    JobStatus::FailedNeedsManual,
                    Some(job.attempt + 1),
                    Some(None),
                    Some(Some(reason.clone())),
                )?;
                db.add_event(
                    Some(&job.id),
                    "poll_terminal_error",
                    json!({ "status": status, "message": body }),
                )?;
                warn!(
                    job_id = %job.id,
                    status,
                    "poll returned terminal review-generation failure; marked failed-needs-manual"
                );
            } else {
                let next = Utc::now() + Duration::minutes(RATE_LIMIT_COOLDOWN_MINUTES);
                db.update_job_state(
                    &job.id,
                    JobStatus::Processing,
                    Some(job.attempt + 1),
                    Some(Some(next)),
                    Some(Some(body.clone())),
                )?;
                db.add_event(
                    Some(&job.id),
                    "poll_server_error",
                    json!({ "status": status, "message": body, "next_poll_at": next.to_rfc3339() }),
                )?;
                warn!(job_id = %job.id, "poll server error; cooldown applied");
            }
        }
        Err(err) => {
            let next = compute_next_poll_at(
                Utc::now(),
                &config.polling.schedule_minutes,
                job.attempt + 1,
                config.polling.jitter_percent,
            );
            db.update_job_state(
                &job.id,
                JobStatus::Processing,
                Some(job.attempt + 1),
                Some(Some(next)),
                Some(Some(err.to_string())),
            )?;
            db.add_event(
                Some(&job.id),
                "poll_error",
                json!({ "error": err.to_string(), "next_poll_at": next.to_rfc3339() }),
            )?;
            warn!(job_id = %job.id, "poll failed; scheduled retry");
        }
    }

    Ok(())
}

pub fn mark_timeouts(config: &Config, db: &Db) -> Result<()> {
    let now = Utc::now();
    let timeout = Duration::hours(config.core.review_timeout_hours as i64);

    for job in db.list_processing_jobs()? {
        if now - job.created_at >= timeout {
            db.update_job_state(
                &job.id,
                JobStatus::Timeout,
                Some(job.attempt),
                Some(None),
                Some(Some("review timed out".to_string())),
            )?;
            db.add_event(Some(&job.id), "timeout", json!({}))?;
            warn!(job_id = %job.id, "job timed out");
        }
    }

    Ok(())
}

pub fn prune_retention(config: &Config, db: &Db, tick: Option<u64>) -> Result<()> {
    if !config.retention.enabled {
        return Ok(());
    }
    if let Some(tick) = tick {
        let interval = config.retention.prune_every_ticks;
        if tick % interval != 0 {
            return Ok(());
        }
    }

    let report = db.prune_retention(&config.retention, Utc::now())?;
    if report.total_deleted() == 0 {
        return Ok(());
    }

    db.add_event(
        None,
        "retention_pruned",
        json!({
            "email_tokens": report.email_tokens,
            "seen_tags": report.seen_tags,
            "events": report.events,
            "reviews": report.reviews,
            "jobs": report.jobs
        }),
    )?;
    info!(
        deleted = report.total_deleted(),
        email_tokens = report.email_tokens,
        seen_tags = report.seen_tags,
        events = report.events,
        reviews = report.reviews,
        jobs = report.jobs,
        "retention pruning deleted stale records"
    );
    Ok(())
}
