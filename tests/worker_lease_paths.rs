//! OSS-337 worker lease paths the acceptance suites leave open:
//! - a primary dispatch that never answers is bounded and parked SUBMITTED/UNCERTAIN,
//!   never handed to the armed fallback;
//! - a worker that loses its submit lease during the primary call never starts the
//!   fallback afterwards;
//! - a local failure before dispatch hands the claim straight back;
//! - `mark_timeouts` waits for a live poll lease and rejects the stale owner afterwards;
//! - a poll in flight excludes every other poller;
//! - a receipt kept for recovery really resumes through `retry` and is not timed out
//!   from the job's creation time.
//!
//! Every competing worker uses its own `Db` handle on one file database. Time passing
//! is simulated with a past or future `now` handed to the Db primitives, and the
//! dispatch timeout with tokio's paused clock.

mod common;

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use common::{
    Answer, Ctx, MockBackend, POLL_TTL, PROJECT, SUBMIT_TTL, assert_no_lease, completed_change,
    count_events, diagnostic, dispatch_channels, event_types, node_available, only_event,
    ready_review, receipt, review_json, with_deadline,
};
use reviewloop::{
    backend::BackendError,
    db::{CancelOutcome, ClaimTiming, Db, LeaseRecovery, LeaseWrite, NewReview, ReceiptWrite},
    model::{Job, JobStatus, SubmitChannel, SubmitStage, WorkKind},
    util::to_rfc3339,
    worker::{self, Attempt},
};
use rusqlite::{OptionalExtension, params};
use serde_json::json;
use std::{path::Path, time::Duration as StdDuration};

/// Mirrors the worker's bound on one dispatch.
const DISPATCH_TIMEOUT: StdDuration = StdDuration::from_secs(20 * 60);

impl Ctx {
    fn set_started_at(&self, job_id: &str, at: DateTime<Utc>) -> Result<()> {
        let changed = self.conn()?.execute(
            "UPDATE jobs SET started_at = ?2 WHERE id = ?1",
            params![job_id, to_rfc3339(at)],
        )?;
        assert_eq!(changed, 1);
        Ok(())
    }

    fn set_created_at(&self, job_id: &str, at: DateTime<Utc>) -> Result<()> {
        let changed = self.conn()?.execute(
            "UPDATE jobs SET created_at = ?2 WHERE id = ?1",
            params![job_id, to_rfc3339(at)],
        )?;
        assert_eq!(changed, 1);
        Ok(())
    }

    /// `(token, raw_json)` of the stored review, if any.
    fn review(&self, job_id: &str) -> Result<Option<(String, String)>> {
        Ok(self
            .conn()?
            .query_row(
                "SELECT token, raw_json FROM reviews WHERE job_id = ?1",
                params![job_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?)
    }
}

/// Primary backend that parks each submit until `release`, then provably rejects it.
fn rejects_on_release() -> MockBackend {
    MockBackend::default()
        .on_submit(|| Answer::OnRelease(Err(BackendError::Schema("upload rejected".to_string()))))
}

/// The marker only proves anything when node could have run the script.
fn assert_fallback_script_never_ran(marker: &Path) {
    if node_available() {
        assert!(!marker.exists(), "the fallback script must not run");
    }
}

/// What `reviewloop retry` does for a job that has a token: back to PROCESSING, polled
/// from now, error cleared.
fn retry_with_token(db: &Db, job_id: &str, now: DateTime<Utc>) -> Result<()> {
    db.update_job_state_unchecked(
        job_id,
        JobStatus::Processing,
        Some(0),
        Some(Some(now)),
        Some(None),
    )
}

// ---------------------------------------------------------------------------
// 1. A primary dispatch that never answers
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn hung_primary_dispatch_times_out_to_uncertain_without_fallback() -> Result<()> {
    let mut ctx = Ctx::new()?;
    // Fully armed, so only the outcome classification keeps the fallback from running.
    let marker = ctx.arm_marker_fallback()?;
    let job = ctx.create_queued_job()?;
    let backend = MockBackend::default().on_submit(|| Answer::Never);

    // No real-time deadline here: under the paused clock it would fire first.
    let clock = tokio::time::Instant::now();
    let attempt = worker::submit_job_with_backend(&ctx.config, &ctx.db, &job.id, &backend).await?;
    let waited = clock.elapsed();
    assert_eq!(attempt, Attempt::Ran);
    assert_eq!(backend.submit_count(), 1);
    assert!(
        waited >= DISPATCH_TIMEOUT && waited < DISPATCH_TIMEOUT + StdDuration::from_secs(60),
        "the dispatch must be cut off at 20 minutes, waited {waited:?}"
    );

    let parked = ctx.job(&job.id)?;
    assert_eq!(parked.status, JobStatus::Submitted);
    assert_eq!(parked.submit_stage, Some(SubmitStage::Uncertain));
    assert_no_lease(&parked);
    assert_eq!(parked.attempt, 1);
    assert_eq!(parked.token, None);
    assert_eq!(parked.started_at, None);
    assert_eq!(parked.next_poll_at, None);
    assert!(!parked.fallback_used, "the fallback must not be dispatched");
    let reason = diagnostic(&parked)?;
    for needle in [
        "outcome unknown (primary channel)",
        "no response within 1200s",
    ] {
        assert!(
            reason.contains(needle),
            "last_error must mention {needle:?}: {reason}"
        );
    }
    assert!(reason.contains(&parked.reconcile_hint()), "{reason}");
    assert_fallback_script_never_ran(&marker);

    let events = ctx.events(&job.id)?;
    assert_eq!(
        event_types(&events),
        vec!["submit_dispatched", "submit_outcome_unknown"]
    );
    assert_eq!(dispatch_channels(&events), vec!["primary"]);
    assert!(only_event(&events, "submit_dispatched")["owner"].is_string());
    assert_eq!(
        only_event(&events, "submit_outcome_unknown"),
        &json!({
            "source": "dispatch_error",
            "channel": "primary",
            "error": "no response within 1200s",
        })
    );

    // Nothing sends it again: by-id submit from another worker, a recovery sweep.
    let other = ctx.other_handle();
    assert_eq!(
        worker::submit_job_with_backend(&ctx.config, &other, &job.id, &backend).await?,
        Attempt::NotClaimed
    );
    assert_eq!(
        other.recover_expired_leases(PROJECT, Utc::now() + Duration::hours(1))?,
        LeaseRecovery::default()
    );
    assert_eq!(backend.submit_count(), 1);
    assert_fallback_script_never_ran(&marker);
    let still = ctx.job(&job.id)?;
    assert_eq!(still.status, JobStatus::Submitted);
    assert_eq!(still.submit_stage, Some(SubmitStage::Uncertain));
    assert_eq!(still.attempt, 1);
    assert_eq!(still.last_error, parked.last_error);
    assert_eq!(event_types(&ctx.events(&job.id)?), event_types(&events));
    Ok(())
}

// ---------------------------------------------------------------------------
// 2. The lease is lost while the primary call is in flight
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cancel_during_primary_call_keeps_fallback_from_starting() -> Result<()> {
    let mut ctx = Ctx::new()?;
    let marker = ctx.arm_marker_fallback()?;
    let job = ctx.create_queued_job()?;
    let backend = rejects_on_release();
    let other = ctx.other_handle();

    let (attempt, cancel) = with_deadline(async {
        tokio::join!(
            worker::submit_job_with_backend(&ctx.config, &ctx.db, &job.id, &backend),
            async {
                backend.entered.notified().await;
                let in_flight = other.get_job(&job.id);
                let outcome = other.cancel_job(&job.id, Some("operator abort"), Utc::now());
                backend.release.notify_one();
                (in_flight, outcome)
            }
        )
    })
    .await?;
    let (in_flight, cancel) = cancel;

    let in_flight = in_flight?.context("job")?;
    assert_eq!(in_flight.status, JobStatus::Submitted);
    assert_eq!(in_flight.submit_stage, Some(SubmitStage::Dispatched));
    assert!(in_flight.lease_owner.is_some());
    assert!(
        !in_flight.fallback_used,
        "the primary dispatch must not set fallback_used"
    );

    assert_eq!(attempt?, Attempt::Ran);
    assert_eq!(
        cancel?,
        CancelOutcome::Cancelled {
            previous_status: JobStatus::Submitted,
            previous_stage: Some(SubmitStage::Dispatched),
            lease_was_active: true,
        }
    );
    assert_eq!(backend.submit_count(), 1);
    assert_fallback_script_never_ran(&marker);

    let after = ctx.job(&job.id)?;
    assert_eq!(after.status, JobStatus::Failed);
    assert_eq!(
        after.last_error.as_deref(),
        Some("cancelled by user: operator abort")
    );
    assert_eq!(after.submit_stage, None);
    assert_no_lease(&after);
    assert!(!after.fallback_used);
    assert_eq!(after.attempt, 0);
    assert_eq!(after.token, None);
    assert_eq!(after.next_poll_at, None);

    // The rejection lands on a lease that is gone: the fallback is skipped, not
    // started, and the stale result is recorded like any other.
    let events = ctx.events(&job.id)?;
    assert_eq!(
        event_types(&events),
        vec!["submit_dispatched", "cancelled", "stale_result_rejected"]
    );
    assert_eq!(dispatch_channels(&events), vec!["primary"]);
    let stale = only_event(&events, "stale_result_rejected");
    assert_eq!(stale["outcome"], "submit_failed");
    assert_eq!(stale["current_status"], "FAILED");
    assert_eq!(count_events(&events, "submit_failed_needs_manual"), 0);

    assert_eq!(
        worker::submit_job_with_backend(&ctx.config, &ctx.db, &job.id, &backend).await?,
        Attempt::NotClaimed
    );
    assert_eq!(backend.submit_count(), 1);
    Ok(())
}

#[tokio::test]
async fn lease_recovered_during_primary_call_keeps_fallback_from_starting() -> Result<()> {
    let mut ctx = Ctx::new()?;
    let marker = ctx.arm_marker_fallback()?;
    let job = ctx.create_queued_job()?;
    let backend = rejects_on_release();
    let other = ctx.other_handle();

    let (attempt, recovery) = with_deadline(async {
        tokio::join!(
            worker::submit_job_with_backend(&ctx.config, &ctx.db, &job.id, &backend),
            async {
                backend.entered.notified().await;
                // Another tick, 31 minutes later, finds the dispatched lease expired.
                let report =
                    other.recover_expired_leases(PROJECT, Utc::now() + Duration::minutes(31));
                backend.release.notify_one();
                report
            }
        )
    })
    .await?;

    assert_eq!(attempt?, Attempt::Ran);
    assert_eq!(
        recovery?,
        LeaseRecovery {
            released_claims: 0,
            uncertain_submits: 1
        }
    );
    assert_eq!(backend.submit_count(), 1);
    assert_fallback_script_never_ran(&marker);

    let after = ctx.job(&job.id)?;
    assert_eq!(after.status, JobStatus::Submitted);
    assert_eq!(after.submit_stage, Some(SubmitStage::Uncertain));
    assert_no_lease(&after);
    assert!(!after.fallback_used);
    assert_eq!(after.attempt, 0);
    assert_eq!(after.token, None);
    let reason = diagnostic(&after)?;
    assert!(reason.contains("lost its lease"), "{reason}");
    assert!(reason.contains(&after.reconcile_hint()), "{reason}");

    let events = ctx.events(&job.id)?;
    assert_eq!(
        event_types(&events),
        vec![
            "submit_dispatched",
            "submit_outcome_unknown",
            "stale_result_rejected"
        ]
    );
    assert_eq!(dispatch_channels(&events), vec!["primary"]);
    assert_eq!(
        only_event(&events, "submit_outcome_unknown")["source"],
        "lease_expired"
    );
    let stale = only_event(&events, "stale_result_rejected");
    assert_eq!(stale["outcome"], "submit_failed");
    assert_eq!(stale["current_status"], "SUBMITTED");

    assert_eq!(
        worker::submit_job_with_backend(&ctx.config, &ctx.db, &job.id, &backend).await?,
        Attempt::NotClaimed
    );
    assert_eq!(backend.submit_count(), 1);
    Ok(())
}

/// Control for the two tests above: the same armed fallback and the same gated
/// rejection do start the fallback when the lease survives the primary call.
#[tokio::test]
async fn gated_primary_rejection_with_lease_kept_runs_fallback() -> Result<()> {
    if !node_available() {
        return Ok(());
    }
    let mut ctx = Ctx::new()?;
    let marker = ctx.arm_marker_fallback()?;
    let job = ctx.create_queued_job()?;
    let backend = rejects_on_release();

    let (attempt, ()) = with_deadline(async {
        tokio::join!(
            worker::submit_job_with_backend(&ctx.config, &ctx.db, &job.id, &backend),
            async {
                backend.entered.notified().await;
                backend.release.notify_one();
            }
        )
    })
    .await?;
    assert_eq!(attempt?, Attempt::Ran);
    assert_eq!(backend.submit_count(), 1);
    assert!(marker.exists(), "the fallback script must run");

    let done = ctx.job(&job.id)?;
    assert_eq!(done.status, JobStatus::Processing);
    assert_eq!(done.token.as_deref(), Some("tok-fallback"));
    assert!(done.fallback_used);
    assert_eq!(done.submit_stage, None);
    assert_no_lease(&done);
    let events = ctx.events(&job.id)?;
    assert_eq!(
        event_types(&events),
        vec![
            "submit_dispatched",
            "submit_dispatched",
            "submitted_via_fallback"
        ]
    );
    assert_eq!(dispatch_channels(&events), vec!["primary", "fallback"]);
    Ok(())
}

/// Without a fallback the same late rejection goes through the guarded finish and is
/// recorded as a rejected stale result.
#[tokio::test]
async fn cancel_during_primary_call_without_fallback_rejects_stale_failure() -> Result<()> {
    let ctx = Ctx::new()?;
    let job = ctx.create_queued_job()?;
    let backend = rejects_on_release();
    let other = ctx.other_handle();

    let (attempt, cancel) = with_deadline(async {
        tokio::join!(
            worker::submit_job_with_backend(&ctx.config, &ctx.db, &job.id, &backend),
            async {
                backend.entered.notified().await;
                let outcome = other.cancel_job(&job.id, None, Utc::now());
                backend.release.notify_one();
                outcome
            }
        )
    })
    .await?;
    assert_eq!(attempt?, Attempt::Ran);
    assert!(matches!(cancel?, CancelOutcome::Cancelled { .. }));
    assert_eq!(backend.submit_count(), 1);

    let after = ctx.job(&job.id)?;
    assert_eq!(after.status, JobStatus::Failed);
    assert_eq!(after.last_error.as_deref(), Some("cancelled by user"));
    assert_eq!(after.attempt, 0);
    assert_eq!(after.submit_stage, None);
    assert_no_lease(&after);
    assert!(!after.fallback_used);

    let events = ctx.events(&job.id)?;
    assert_eq!(
        event_types(&events),
        vec!["submit_dispatched", "cancelled", "stale_result_rejected"]
    );
    let rejected = only_event(&events, "stale_result_rejected");
    assert_eq!(rejected["kind"], "submit");
    assert_eq!(rejected["outcome"], "submit_failed");
    assert_eq!(rejected["current_status"], "FAILED");
    assert_eq!(
        rejected["owner"],
        only_event(&events, "submit_dispatched")["owner"]
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// 3. A local failure before dispatch
// ---------------------------------------------------------------------------

fn assert_claim_released(ctx: &Ctx, job: &Job) -> Result<()> {
    let row = ctx.job(&job.id)?;
    assert_eq!(row.status, JobStatus::Queued);
    assert_eq!(row.submit_stage, None);
    assert_no_lease(&row);
    assert_eq!(row.attempt, 0);
    assert_eq!(row.last_error, None);
    assert_eq!(row.next_poll_at, job.next_poll_at);
    assert!(
        ctx.events(&job.id)?.is_empty(),
        "nothing was dispatched or recorded"
    );
    Ok(())
}

#[tokio::test]
async fn unresolvable_email_releases_submit_claim_before_dispatch() -> Result<()> {
    let mut ctx = Ctx::new()?;
    let job = ctx.create_queued_job()?;
    // No submitter email anywhere: the job's, the config's and the account store's.
    ctx.conn()?
        .execute("UPDATE jobs SET email = '' WHERE id = ?1", [&job.id])?;
    ctx.config.providers.stanford.email.clear();
    let job = ctx.job(&job.id)?;
    let backend = MockBackend::default();

    let err = worker::submit_job_with_backend(&ctx.config, &ctx.db, &job.id, &backend)
        .await
        .expect_err("a job without a submitter email cannot be submitted");
    let message = format!("{err:#}");
    assert!(message.contains("no email available"), "{message}");
    assert_eq!(backend.submit_count(), 0);
    assert_claim_released(&ctx, &job)?;

    // The daemon path hands the claim back the same way.
    let err = worker::process_submissions(&ctx.config, &ctx.db)
        .await
        .expect_err("the daemon cannot submit a job without a submitter email");
    let message = format!("{err:#}");
    assert!(message.contains("no email available"), "{message}");
    assert_claim_released(&ctx, &job)?;

    // Claimable again at once, with no wait for a lease to expire.
    let now = Utc::now();
    let lease = ctx
        .other_handle()
        .claim_job(
            &job.id,
            WorkKind::Submit,
            ClaimTiming::WhenDue,
            now,
            SUBMIT_TTL,
        )?
        .context("a released claim must be claimable right away")?;
    assert_eq!(lease.job.status, JobStatus::Queued);
    assert_eq!(lease.job.submit_stage, Some(SubmitStage::Claimed));
    assert_eq!(lease.job.lease_owner.as_deref(), Some(lease.owner.as_str()));
    assert_eq!(lease.expires_at, now + SUBMIT_TTL);
    assert!(
        ctx.events(&job.id)?.is_empty(),
        "a released claim is not a takeover"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// 4. mark_timeouts and poll leases
// ---------------------------------------------------------------------------

/// A PROCESSING job well past a one-hour review timeout.
fn overdue_processing_job(ctx: &mut Ctx, token: &str) -> Result<Job> {
    ctx.config.core.review_timeout_hours = 1;
    let job = ctx.create_processing_job(token, Utc::now())?;
    ctx.set_started_at(&job.id, Utc::now() - Duration::days(10))?;
    ctx.job(&job.id)
}

#[test]
fn mark_timeouts_waits_for_live_poll_lease_then_rejects_stale_owner() -> Result<()> {
    let mut ctx = Ctx::new()?;
    let job = overdue_processing_job(&mut ctx, "tok-overdue")?;
    let poller = ctx.other_handle();
    let lease = poller
        .claim_job(
            &job.id,
            WorkKind::Poll,
            ClaimTiming::Now,
            Utc::now(),
            POLL_TTL,
        )?
        .context("a PROCESSING job must be claimable for poll")?;

    worker::mark_timeouts(&ctx.config, &ctx.db)?;
    let held = ctx.job(&job.id)?;
    assert_eq!(held.status, JobStatus::Processing);
    assert_eq!(held.lease_owner.as_deref(), Some(lease.owner.as_str()));
    assert_eq!(held.lease_expires_at, Some(lease.expires_at));
    assert_eq!(held.last_error, None);
    assert_eq!(held.attempt, 0);
    assert!(
        ctx.events(&job.id)?.is_empty(),
        "no timeout under a live lease"
    );

    assert!(poller.release_lease(&lease)?);
    worker::mark_timeouts(&ctx.config, &ctx.db)?;
    let timed_out = ctx.job(&job.id)?;
    assert_eq!(timed_out.status, JobStatus::Timeout);
    assert_eq!(timed_out.last_error.as_deref(), Some("review timed out"));
    assert_eq!(timed_out.next_poll_at, None);
    assert_eq!(timed_out.attempt, 0);
    assert_eq!(timed_out.token.as_deref(), Some("tok-overdue"));
    assert_eq!(timed_out.submit_stage, None);
    assert_no_lease(&timed_out);
    let events = ctx.events(&job.id)?;
    assert_eq!(event_types(&events), vec!["timeout"]);
    assert_eq!(only_event(&events, "timeout"), &json!({}));

    // The poll owner's late result is rejected and stores no review.
    let raw = review_json().to_string();
    assert_eq!(
        poller.finish_lease_with_review(
            &lease,
            Utc::now(),
            NewReview {
                token: "tok-overdue",
                raw_json: &raw,
                summary_md: "late",
            },
            &completed_change(),
            "review_completed",
            json!({ "token": "tok-overdue" }),
        )?,
        LeaseWrite::Lost(Some(JobStatus::Timeout))
    );
    assert_eq!(
        poller.finish_lease(
            &lease,
            Utc::now(),
            &completed_change(),
            "review_completed",
            json!({}),
        )?,
        LeaseWrite::Lost(Some(JobStatus::Timeout))
    );
    assert_eq!(ctx.review(&job.id)?, None);
    let still = ctx.job(&job.id)?;
    assert_eq!(still.status, JobStatus::Timeout);
    assert_eq!(still.last_error, timed_out.last_error);

    // A second sweep is a no-op.
    worker::mark_timeouts(&ctx.config, &ctx.db)?;
    assert_eq!(event_types(&ctx.events(&job.id)?), vec!["timeout"]);
    Ok(())
}

#[test]
fn poll_in_flight_on_overdue_job_finishes_before_timeout_sweep() -> Result<()> {
    let mut ctx = Ctx::new()?;
    let job = overdue_processing_job(&mut ctx, "tok-overdue-ready")?;
    let poller = ctx.other_handle();
    let lease = poller
        .claim_job(
            &job.id,
            WorkKind::Poll,
            ClaimTiming::Now,
            Utc::now(),
            POLL_TTL,
        )?
        .context("a PROCESSING job must be claimable for poll")?;

    worker::mark_timeouts(&ctx.config, &ctx.db)?;
    assert_eq!(ctx.job(&job.id)?.status, JobStatus::Processing);

    let raw = review_json().to_string();
    assert_eq!(
        poller.finish_lease_with_review(
            &lease,
            Utc::now(),
            NewReview {
                token: "tok-overdue-ready",
                raw_json: &raw,
                summary_md: "ready",
            },
            &completed_change(),
            "review_completed",
            json!({ "token": "tok-overdue-ready" }),
        )?,
        LeaseWrite::Applied
    );

    worker::mark_timeouts(&ctx.config, &ctx.db)?;
    let done = ctx.job(&job.id)?;
    assert_eq!(done.status, JobStatus::Completed);
    assert_eq!(done.last_error, None);
    assert_no_lease(&done);
    assert_eq!(
        ctx.review(&job.id)?,
        Some(("tok-overdue-ready".to_string(), raw))
    );
    assert_eq!(event_types(&ctx.events(&job.id)?), vec!["review_completed"]);
    Ok(())
}

#[test]
fn expired_poll_lease_does_not_shield_overdue_job() -> Result<()> {
    let mut ctx = Ctx::new()?;
    let job = overdue_processing_job(&mut ctx, "tok-overdue-stalled")?;
    let poller = ctx.other_handle();
    // The poller claimed 11 minutes ago and stalled past its 10-minute lease.
    let lease = poller
        .claim_job(
            &job.id,
            WorkKind::Poll,
            ClaimTiming::Now,
            Utc::now() - Duration::minutes(11),
            POLL_TTL,
        )?
        .context("a PROCESSING job must be claimable for poll")?;
    assert!(lease.expires_at < Utc::now());

    worker::mark_timeouts(&ctx.config, &ctx.db)?;
    let timed_out = ctx.job(&job.id)?;
    assert_eq!(timed_out.status, JobStatus::Timeout);
    assert_eq!(timed_out.last_error.as_deref(), Some("review timed out"));
    assert_no_lease(&timed_out);
    assert_eq!(event_types(&ctx.events(&job.id)?), vec!["timeout"]);

    assert_eq!(
        poller.finish_lease(
            &lease,
            Utc::now(),
            &completed_change(),
            "review_completed",
            json!({}),
        )?,
        LeaseWrite::Lost(Some(JobStatus::Timeout))
    );
    assert_eq!(ctx.job(&job.id)?.status, JobStatus::Timeout);
    Ok(())
}

// ---------------------------------------------------------------------------
// 5. Worker-level poll exclusion
// ---------------------------------------------------------------------------

/// Everything another worker can try while the owner is parked inside fetch_review.
async fn compete_for_poll(
    ctx: &Ctx,
    other: &Db,
    job_id: &str,
    backend: &MockBackend,
) -> Result<()> {
    let now = Utc::now();
    let row = ctx.job(job_id)?;
    assert_eq!(row.status, JobStatus::Processing);
    assert!(row.lease_owner.is_some());
    assert!(row.lease_expires_at.is_some_and(|at| at > now));
    assert!(
        row.next_poll_at.is_some_and(|at| at <= now),
        "the job is due"
    );

    assert_eq!(
        worker::poll_job_with_backend(&ctx.config, other, job_id, backend).await?,
        Attempt::NotClaimed
    );
    assert!(
        other
            .claim_job(job_id, WorkKind::Poll, ClaimTiming::Now, now, POLL_TTL)?
            .is_none()
    );
    assert!(other.list_due_processing(PROJECT, 10, now)?.is_empty());
    worker::process_polls(&ctx.config, other).await?;
    assert_eq!(backend.fetch_count(), 1);

    let still = ctx.job(job_id)?;
    assert_eq!(still.status, JobStatus::Processing);
    assert_eq!(still.lease_owner, row.lease_owner);
    assert_eq!(still.lease_expires_at, row.lease_expires_at);
    assert_eq!(still.attempt, 0);
    Ok(())
}

#[tokio::test]
async fn poll_in_flight_excludes_second_worker_and_completes_once() -> Result<()> {
    let ctx = Ctx::new()?;
    let job = ctx.create_processing_job("tok-exclusive", Utc::now())?;
    let backend = MockBackend::default().on_fetch(|| Answer::OnRelease(Ok(ready_review())));
    let other = ctx.other_handle();

    let (attempt, competitor) = with_deadline(async {
        tokio::join!(
            worker::poll_job_with_backend(&ctx.config, &ctx.db, &job.id, &backend),
            async {
                backend.entered.notified().await;
                let observed = compete_for_poll(&ctx, &other, &job.id, &backend).await;
                // Release unconditionally so a failed check cannot hang the parked poll.
                backend.release.notify_one();
                observed
            }
        )
    })
    .await?;
    competitor?;
    assert_eq!(attempt?, Attempt::Ran);
    assert_eq!(backend.fetch_count(), 1);
    assert_eq!(backend.fetched_tokens(), vec!["tok-exclusive"]);

    let done = ctx.job(&job.id)?;
    assert_eq!(done.status, JobStatus::Completed);
    assert_eq!(done.token.as_deref(), Some("tok-exclusive"));
    assert_eq!(done.attempt, 1);
    assert_eq!(done.next_poll_at, None);
    assert_eq!(done.last_error, None);
    assert_eq!(done.submit_stage, None);
    assert_no_lease(&done);
    assert_eq!(
        ctx.review(&job.id)?,
        Some(("tok-exclusive".to_string(), review_json().to_string()))
    );

    let events = ctx.events(&job.id)?;
    assert_eq!(event_types(&events), vec!["review_completed"]);
    assert_eq!(
        only_event(&events, "review_completed"),
        &json!({ "token": "tok-exclusive" })
    );
    assert_eq!(count_events(&events, "stale_result_rejected"), 0);
    Ok(())
}

// ---------------------------------------------------------------------------
// 6. A receipt kept for recovery resumes through retry
// ---------------------------------------------------------------------------

/// Retry a job holding a kept token, prove an old `created_at` does not time it out,
/// then poll it to completion with that token.
async fn retry_then_complete(ctx: &Ctx, job_id: &str, token: &str) -> Result<Job> {
    let kept = ctx.job(job_id)?;
    let started_at = kept.started_at;
    assert!(
        started_at.is_some(),
        "a kept token must come with started_at"
    );

    // The job itself is old; the review clock counts from the receipt.
    ctx.set_created_at(job_id, Utc::now() - Duration::days(10))?;
    worker::mark_timeouts(&ctx.config, &ctx.db)?;
    assert_eq!(ctx.job(job_id)?.status, kept.status);

    let now = Utc::now();
    retry_with_token(&ctx.db, job_id, now)?;
    let resumed = ctx.job(job_id)?;
    assert_eq!(resumed.status, JobStatus::Processing);
    assert_eq!(resumed.token.as_deref(), Some(token));
    assert_eq!(resumed.submit_stage, None);
    assert_no_lease(&resumed);
    assert_eq!(resumed.last_error, None);
    assert_eq!(resumed.attempt, 0);
    assert_eq!(resumed.next_poll_at, Some(now));
    assert_eq!(resumed.started_at, started_at);
    assert!(resumed.created_at < now - Duration::days(9));

    worker::mark_timeouts(&ctx.config, &ctx.db)?;
    let not_timed_out = ctx.job(job_id)?;
    assert_eq!(not_timed_out.status, JobStatus::Processing);
    assert_eq!(not_timed_out.last_error, None);
    assert_eq!(count_events(&ctx.events(job_id)?, "timeout"), 0);

    let backend = MockBackend::default().on_fetch(|| Answer::Now(Ok(ready_review())));
    assert_eq!(
        worker::poll_job_with_backend(&ctx.config, &ctx.db, job_id, &backend).await?,
        Attempt::Ran
    );
    assert_eq!(backend.fetched_tokens(), vec![token]);
    assert_eq!(backend.submit_count(), 0);

    let done = ctx.job(job_id)?;
    assert_eq!(done.status, JobStatus::Completed);
    assert_eq!(done.token.as_deref(), Some(token));
    assert_eq!(done.attempt, 1);
    assert_eq!(done.next_poll_at, None);
    assert_eq!(done.last_error, None);
    assert_eq!(done.submit_stage, None);
    assert_no_lease(&done);
    assert_eq!(
        ctx.review(job_id)?,
        Some((token.to_string(), review_json().to_string()))
    );
    let events = ctx.events(job_id)?;
    assert_eq!(
        only_event(&events, "review_completed"),
        &json!({ "token": token })
    );
    assert_eq!(count_events(&events, "stale_result_rejected"), 0);
    Ok(done)
}

#[tokio::test]
async fn receipt_kept_on_uncertain_job_resumes_through_retry() -> Result<()> {
    let mut ctx = Ctx::new()?;
    ctx.config.core.review_timeout_hours = 1;
    let job = ctx.create_queued_job()?;

    // A worker dispatched 40 minutes ago and stalled past its 30-minute lease.
    let t0 = Utc::now() - Duration::minutes(40);
    let mut lease = ctx
        .db
        .claim_job(&job.id, WorkKind::Submit, ClaimTiming::Now, t0, SUBMIT_TTL)?
        .context("a fresh QUEUED job must be claimable")?;
    assert!(
        ctx.db
            .begin_submit_dispatch(&mut lease, SubmitChannel::Primary, t0, SUBMIT_TTL)?
    );
    let other = ctx.other_handle();
    assert_eq!(
        other.recover_expired_leases(PROJECT, Utc::now())?,
        LeaseRecovery {
            released_claims: 0,
            uncertain_submits: 1
        }
    );

    // Its receipt finally arrives.
    let arrived = Utc::now();
    assert_eq!(
        ctx.db.record_submit_receipt(
            &lease,
            arrived,
            "tok-kept",
            arrived + Duration::minutes(10),
            SubmitChannel::Primary,
        )?,
        ReceiptWrite::StoredForRecovery
    );
    let parked = ctx.job(&job.id)?;
    assert_eq!(parked.status, JobStatus::Submitted);
    assert_eq!(parked.submit_stage, Some(SubmitStage::Uncertain));
    assert_eq!(parked.token.as_deref(), Some("tok-kept"));
    assert_eq!(parked.started_at, Some(arrived));
    assert_no_lease(&parked);
    let reason = diagnostic(&parked)?;
    assert!(reason.contains(&parked.reconcile_hint()), "{reason}");
    assert!(
        reason.contains(&format!("reviewloop retry --job-id {}", job.id)),
        "{reason}"
    );

    retry_then_complete(&ctx, &job.id, "tok-kept").await?;
    assert_eq!(
        event_types(&ctx.events(&job.id)?),
        vec![
            "submit_dispatched",
            "submit_outcome_unknown",
            "submit_receipt_after_lease_lost",
            "review_completed"
        ]
    );
    Ok(())
}

#[tokio::test]
async fn receipt_kept_on_cancelled_job_resumes_through_retry() -> Result<()> {
    let mut ctx = Ctx::new()?;
    ctx.config.core.review_timeout_hours = 1;
    let job = ctx.create_queued_job()?;
    let backend =
        MockBackend::default().on_submit(|| Answer::OnRelease(Ok(receipt("tok-cancel-kept"))));
    let other = ctx.other_handle();

    let before = Utc::now();
    let (attempt, cancel) = with_deadline(async {
        tokio::join!(
            worker::submit_job_with_backend(&ctx.config, &ctx.db, &job.id, &backend),
            async {
                backend.entered.notified().await;
                let outcome = other.cancel_job(&job.id, Some("operator abort"), Utc::now());
                backend.release.notify_one();
                outcome
            }
        )
    })
    .await?;
    let after = Utc::now();
    assert_eq!(attempt?, Attempt::Ran);
    assert_eq!(
        cancel?,
        CancelOutcome::Cancelled {
            previous_status: JobStatus::Submitted,
            previous_stage: Some(SubmitStage::Dispatched),
            lease_was_active: true,
        }
    );
    assert_eq!(backend.submit_count(), 1);

    let cancelled = ctx.job(&job.id)?;
    assert_eq!(cancelled.status, JobStatus::Failed);
    assert_eq!(
        cancelled.last_error.as_deref(),
        Some("cancelled by user: operator abort")
    );
    assert_eq!(cancelled.token.as_deref(), Some("tok-cancel-kept"));
    assert!(
        cancelled
            .started_at
            .is_some_and(|at| at >= before && at <= after),
        "started_at must be stamped at receipt time: {:?}",
        cancelled.started_at
    );
    assert_eq!(cancelled.submit_stage, None);
    assert_no_lease(&cancelled);
    let events = ctx.events(&job.id)?;
    let late = only_event(&events, "submit_receipt_after_lease_lost");
    assert_eq!(late["stored"], true);
    assert_eq!(late["status"], "FAILED");
    assert_eq!(late["token"], "tok-cancel-kept");

    retry_then_complete(&ctx, &job.id, "tok-cancel-kept").await?;
    assert_eq!(
        event_types(&ctx.events(&job.id)?),
        vec![
            "submit_dispatched",
            "cancelled",
            "submit_receipt_after_lease_lost",
            "review_completed"
        ]
    );
    // Retrying the poll never resubmitted.
    assert_eq!(backend.submit_count(), 1);
    Ok(())
}

/// OSS-352: a receipt the database cannot save goes to a private recovery file, and the
/// token never appears in the error that reaches the daemon log and notifications.
#[tokio::test]
async fn unsaved_receipt_goes_to_a_private_recovery_file_not_the_log() -> Result<()> {
    const TOKEN: &str = "tok-unsaved-0123456789";
    let ctx = Ctx::new()?;
    let job = ctx.create_queued_job()?;
    let backend = MockBackend::default().on_submit(|| Answer::OnRelease(Ok(receipt(TOKEN))));

    let (attempt, broken) = with_deadline(async {
        tokio::join!(
            worker::submit_job_with_backend(&ctx.config, &ctx.db, &job.id, &backend),
            async {
                backend.entered.notified().await;
                // The receipt's event insert now fails, which rolls back the whole save.
                let broken = ctx
                    .conn()
                    .and_then(|conn| Ok(conn.execute_batch("DROP TABLE events")?));
                backend.release.notify_one();
                broken
            }
        )
    })
    .await?;
    broken?;

    let message = format!("{:#}", attempt.expect_err("saving the receipt must fail"));
    assert!(
        !message.contains(TOKEN),
        "token leaked into the error: {message}"
    );
    let recovery_dir = ctx.config.state_dir().join("recovery");
    let files: Vec<_> = std::fs::read_dir(&recovery_dir)?.collect::<std::io::Result<_>>()?;
    assert_eq!(files.len(), 1, "{files:?}");
    let path = files[0].path();
    assert!(
        message.contains(&path.display().to_string()),
        "the error must point at the recovery file: {message}"
    );
    let saved: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(&path)?)?;
    assert_eq!(saved["token"], TOKEN);
    assert_eq!(saved["job_id"], job.id.as_str());
    assert_eq!(saved["channel"], "primary");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&path)?.permissions().mode() & 0o777,
            0o600
        );
    }
    Ok(())
}
