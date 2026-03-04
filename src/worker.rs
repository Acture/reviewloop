use crate::{
    artifact::write_review_artifacts,
    backend::{BackendError, ReviewFetchResult, SubmitRequest, build_backend},
    config::Config,
    db::Db,
    email::poll_imap_if_enabled,
    fallback::submit_with_node_playwright,
    model::{Job, JobStatus},
    trigger::{run_git_tag_trigger, run_pdf_trigger},
    util::compute_next_poll_at,
};
use anyhow::{Context, Result};
use chrono::{Duration, Utc};
use serde_json::json;
use std::{path::Path, time::Duration as StdDuration};

const RATE_LIMIT_COOLDOWN_MINUTES: i64 = 30;

pub async fn run_daemon(config: &Config, db: &Db) -> Result<()> {
    loop {
        if let Err(err) = run_tick(config, db).await {
            eprintln!("tick error: {err:#}");
        }

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                eprintln!("received Ctrl+C, daemon exiting");
                break;
            }
            _ = tokio::time::sleep(StdDuration::from_secs(30)) => {}
        }
    }

    Ok(())
}

pub async fn run_tick(config: &Config, db: &Db) -> Result<()> {
    run_git_tag_trigger(config, db)?;
    run_pdf_trigger(config, db)?;

    poll_imap_if_enabled(config, db).await?;

    mark_timeouts(config, db)?;
    process_submissions(config, db).await?;
    process_polls(config, db).await?;

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
        }
        Err(BackendError::Server { status: _, body }) => {
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
                json!({ "message": body, "next_poll_at": next.to_rfc3339() }),
            )?;
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
        }
    }

    Ok(())
}
