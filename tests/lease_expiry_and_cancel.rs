//! OSS-337 acceptance (2) and (3): a worker whose lease expired or was revoked cannot
//! write its result, cancel racing completion has exactly one winner, and a submit
//! receipt that arrives after the lease is gone is kept for recovery without reviving
//! the job.
//!
//! Time passing is simulated by handing a future `now` to the Db primitives; concurrent
//! writers use separate `Db` handles on one file database.

mod common;

use anyhow::{Context, Result};
use chrono::{Duration, Utc};
use common::{
    Answer, Ctx, MockBackend, assert_held_by, assert_no_lease, completed_change, count_events,
    event_types, only_event, ready_review, receipt, with_deadline,
};
use reviewloop::{
    db::{CancelOutcome, ClaimTiming, Db, JobChange, LeaseWrite, NewReview, ReceiptWrite},
    model::{Job, JobStatus, SubmitChannel, SubmitStage, WorkKind},
    worker::{self, Attempt},
};
use rusqlite::{OptionalExtension, params};
use serde_json::{Value, json};
use std::{path::Path, sync::Barrier};

const RACE_ROUNDS: usize = 50;

impl Ctx {
    fn review_raw_json(&self, job_id: &str) -> Result<Option<String>> {
        Ok(self
            .conn()?
            .query_row(
                "SELECT raw_json FROM reviews WHERE job_id = ?1",
                params![job_id],
                |row| row.get(0),
            )
            .optional()?)
    }

    fn review_count(&self, job_id: &str) -> Result<i64> {
        Ok(self.conn()?.query_row(
            "SELECT COUNT(*) FROM reviews WHERE job_id = ?1",
            params![job_id],
            |row| row.get(0),
        )?)
    }
}

fn change(status: JobStatus, stage: Option<SubmitStage>, last_error: Option<&str>) -> JobChange {
    JobChange {
        status,
        attempt: Some(1),
        next_poll_at: Some(None),
        last_error: Some(last_error.map(str::to_string)),
        submit_stage: stage,
        fallback_used: None,
    }
}

fn review<'a>(token: &'a str, raw_json: &'a str) -> NewReview<'a> {
    NewReview {
        token,
        raw_json,
        summary_md: "# summary",
    }
}

/// The execution-state columns a stale owner must not touch.
fn execution_state(job: &Job) -> impl PartialEq + std::fmt::Debug {
    (
        job.status,
        job.token.clone(),
        job.attempt,
        job.next_poll_at,
        job.last_error.clone(),
        job.lease_owner.clone(),
        job.lease_expires_at,
        job.submit_stage,
        job.fallback_used,
        job.updated_at,
    )
}

// ---------------------------------------------------------------------------
// (2) After a lease expires, the old owner's results are rejected.
// ---------------------------------------------------------------------------

#[test]
fn expired_submit_lease_rejects_old_owner_and_hands_job_to_new_claimer() -> Result<()> {
    let ctx = Ctx::new()?;
    let job = ctx.create_queued_job()?;
    let t0 = Utc::now();

    let mut lease_a = ctx
        .db
        .claim_job(
            &job.id,
            WorkKind::Submit,
            ClaimTiming::Now,
            t0,
            Duration::minutes(1),
        )?
        .context("A must claim a fresh QUEUED job")?;
    assert_eq!(lease_a.expires_at, t0 + Duration::minutes(1));
    let claimed = ctx.job(&job.id)?;
    assert_eq!(claimed.status, JobStatus::Queued);
    assert_eq!(claimed.submit_stage, Some(SubmitStage::Claimed));
    assert_held_by(&claimed, &lease_a);

    // A live claim cannot be taken.
    assert!(
        ctx.other_handle()
            .claim_job(
                &job.id,
                WorkKind::Submit,
                ClaimTiming::Now,
                t0 + Duration::seconds(30),
                Duration::minutes(30),
            )?
            .is_none(),
        "a live claim must not be shared"
    );

    // A's lease lapsed: its result is rejected and the row is untouched.
    let t1 = t0 + Duration::minutes(2);
    let before = ctx.job(&job.id)?;
    let write = ctx.db.finish_lease(
        &lease_a,
        t1,
        &change(JobStatus::Failed, None, Some("stale failure from A")),
        "submit_failed",
        json!({ "reason": "stale failure from A" }),
    )?;
    assert_eq!(write, LeaseWrite::Lost(Some(JobStatus::Queued)));
    assert_eq!(
        execution_state(&ctx.job(&job.id)?),
        execution_state(&before)
    );
    assert!(
        !ctx.db.begin_submit_dispatch(
            &mut lease_a,
            SubmitChannel::Primary,
            t1,
            Duration::minutes(30)
        )?,
        "an expired owner must not dispatch even before anyone takes over"
    );
    assert_eq!(
        execution_state(&ctx.job(&job.id)?),
        execution_state(&before)
    );

    // B takes over the expired claim.
    let db_b = ctx.other_handle();
    let mut lease_b = db_b
        .claim_job(
            &job.id,
            WorkKind::Submit,
            ClaimTiming::Now,
            t1,
            Duration::minutes(30),
        )?
        .context("B must claim once A's lease expired")?;
    assert_ne!(lease_b.owner, lease_a.owner);
    let taken = ctx.job(&job.id)?;
    assert_eq!(taken.status, JobStatus::Queued);
    assert_eq!(taken.submit_stage, Some(SubmitStage::Claimed));
    assert_held_by(&taken, &lease_b);

    let events = ctx.events(&job.id)?;
    let takeover = only_event(&events, "submit_claim_taken_over");
    assert_eq!(takeover["previous_owner"], json!(lease_a.owner));
    assert_eq!(takeover["owner"], json!(lease_b.owner));
    assert_eq!(count_events(&events, "submit_failed"), 0);

    // A can neither dispatch, release, nor finish — even at a time its own lease would
    // still have covered.
    let a_view_before = (lease_a.expires_at, lease_a.job.status);
    assert!(!ctx.db.begin_submit_dispatch(
        &mut lease_a,
        SubmitChannel::Primary,
        t1,
        Duration::minutes(30)
    )?);
    assert_eq!((lease_a.expires_at, lease_a.job.status), a_view_before);
    assert!(!ctx.db.release_lease(&lease_a)?);
    assert_eq!(
        ctx.db.finish_lease(
            &lease_a,
            t0 + Duration::seconds(30),
            &change(JobStatus::Failed, None, Some("stale failure from A")),
            "submit_failed",
            json!({}),
        )?,
        LeaseWrite::Lost(Some(JobStatus::Queued))
    );
    let after_a = ctx.job(&job.id)?;
    assert_eq!(execution_state(&after_a), execution_state(&taken));
    assert_eq!(count_events(&ctx.events(&job.id)?, "submit_dispatched"), 0);

    // B owns the job: dispatch and finish apply.
    assert!(db_b.begin_submit_dispatch(
        &mut lease_b,
        SubmitChannel::Primary,
        t1,
        Duration::minutes(30)
    )?);
    let dispatched = ctx.job(&job.id)?;
    assert_eq!(dispatched.status, JobStatus::Submitted);
    assert_eq!(dispatched.submit_stage, Some(SubmitStage::Dispatched));
    assert_held_by(&dispatched, &lease_b);

    let write = db_b.finish_lease(
        &lease_b,
        t1 + Duration::seconds(5),
        &change(JobStatus::Failed, None, Some("rejected by provider")),
        "submit_failed",
        json!({ "reason": "rejected by provider" }),
    )?;
    assert_eq!(write, LeaseWrite::Applied);
    let finished = ctx.job(&job.id)?;
    assert_eq!(finished.status, JobStatus::Failed);
    assert_eq!(finished.attempt, 1);
    assert_eq!(finished.last_error.as_deref(), Some("rejected by provider"));
    assert_eq!(finished.submit_stage, None);
    assert_no_lease(&finished);

    let events = ctx.events(&job.id)?;
    let dispatched = only_event(&events, "submit_dispatched");
    assert_eq!(dispatched["owner"], json!(lease_b.owner));
    assert_eq!(dispatched["channel"], json!("primary"));
    assert_eq!(count_events(&events, "submit_failed"), 1);
    Ok(())
}

#[test]
fn expired_poll_lease_rejects_old_review_and_applies_new_owner_review() -> Result<()> {
    let ctx = Ctx::new()?;
    let t0 = Utc::now();
    let job = ctx.create_processing_job("tok-poll", t0)?;

    let lease_a = ctx
        .db
        .claim_job(
            &job.id,
            WorkKind::Poll,
            ClaimTiming::WhenDue,
            t0,
            Duration::minutes(1),
        )?
        .context("A must claim a due PROCESSING job")?;
    assert_held_by(&ctx.job(&job.id)?, &lease_a);
    assert_eq!(ctx.job(&job.id)?.submit_stage, None);

    let db_b = ctx.other_handle();
    assert!(
        db_b.claim_job(
            &job.id,
            WorkKind::Poll,
            ClaimTiming::Now,
            t0 + Duration::seconds(59),
            Duration::minutes(10)
        )?
        .is_none(),
        "a live poll lease must not be shared"
    );

    let t1 = t0 + Duration::minutes(2);
    let lease_b = db_b
        .claim_job(
            &job.id,
            WorkKind::Poll,
            ClaimTiming::WhenDue,
            t1,
            Duration::minutes(10),
        )?
        .context("B must claim once A's poll lease expired")?;
    assert_ne!(lease_b.owner, lease_a.owner);
    let before = ctx.job(&job.id)?;
    assert_held_by(&before, &lease_b);

    let write = ctx.db.finish_lease_with_review(
        &lease_a,
        t1,
        review("tok-poll", r#"{"from":"A"}"#),
        &completed_change(),
        "review_completed",
        json!({ "token": "tok-poll" }),
    )?;
    assert_eq!(write, LeaseWrite::Lost(Some(JobStatus::Processing)));
    assert_eq!(
        ctx.review_count(&job.id)?,
        0,
        "stale review must not be stored"
    );
    assert_eq!(
        execution_state(&ctx.job(&job.id)?),
        execution_state(&before)
    );
    assert_eq!(count_events(&ctx.events(&job.id)?, "review_completed"), 0);

    let write = db_b.finish_lease_with_review(
        &lease_b,
        t1 + Duration::seconds(5),
        review("tok-poll", r#"{"from":"B"}"#),
        &completed_change(),
        "review_completed",
        json!({ "token": "tok-poll" }),
    )?;
    assert_eq!(write, LeaseWrite::Applied);
    let done = ctx.job(&job.id)?;
    assert_eq!(done.status, JobStatus::Completed);
    assert_eq!(done.attempt, 1);
    assert_no_lease(&done);
    assert_eq!(
        ctx.review_raw_json(&job.id)?.as_deref(),
        Some(r#"{"from":"B"}"#)
    );

    // A replaying its result later still cannot overwrite B's review.
    let write = ctx.db.finish_lease_with_review(
        &lease_a,
        t0 + Duration::seconds(10),
        review("tok-poll", r#"{"from":"A"}"#),
        &completed_change(),
        "review_completed",
        json!({ "token": "tok-poll" }),
    )?;
    assert_eq!(write, LeaseWrite::Lost(Some(JobStatus::Completed)));
    assert_eq!(
        ctx.review_raw_json(&job.id)?.as_deref(),
        Some(r#"{"from":"B"}"#)
    );
    assert_eq!(count_events(&ctx.events(&job.id)?, "review_completed"), 1);
    Ok(())
}

// ---------------------------------------------------------------------------
// Receipts that arrive after the lease is gone are kept for recovery.
// ---------------------------------------------------------------------------

#[test]
fn receipt_after_dispatch_lease_expired_is_stored_for_recovery_without_status_change() -> Result<()>
{
    let ctx = Ctx::new()?;
    let job = ctx.create_queued_job()?;
    let t0 = Utc::now();

    let mut lease = ctx
        .db
        .claim_job(
            &job.id,
            WorkKind::Submit,
            ClaimTiming::Now,
            t0,
            Duration::minutes(30),
        )?
        .context("claim")?;
    assert!(ctx.db.begin_submit_dispatch(
        &mut lease,
        SubmitChannel::Primary,
        t0,
        Duration::minutes(1)
    )?);
    assert_eq!(lease.expires_at, t0 + Duration::minutes(1));
    assert_eq!(lease.job.status, JobStatus::Submitted);
    assert_eq!(lease.job.submit_stage, Some(SubmitStage::Dispatched));

    let t1 = t0 + Duration::minutes(2);
    let next_poll = t1 + Duration::minutes(10);
    let write = ctx.db.record_submit_receipt(
        &lease,
        t1,
        "tok-late-1",
        next_poll,
        SubmitChannel::Primary,
    )?;
    assert_eq!(write, ReceiptWrite::StoredForRecovery);

    let stored = ctx.job(&job.id)?;
    assert_eq!(
        stored.status,
        JobStatus::Submitted,
        "status must not change"
    );
    assert_eq!(stored.submit_stage, Some(SubmitStage::Uncertain));
    assert_eq!(stored.token.as_deref(), Some("tok-late-1"));
    assert_eq!(stored.attempt, 0);
    assert_eq!(
        stored.next_poll_at, None,
        "receipt must not schedule a poll"
    );
    assert_no_lease(&stored);
    let last_error = stored.last_error.as_deref().unwrap_or_default();
    assert!(
        last_error.contains("receipt arrived after the worker lost its lease")
            && last_error.contains("a receipt token is saved"),
        "unexpected last_error: {last_error}"
    );

    let events = ctx.events(&job.id)?;
    let late = only_event(&events, "submit_receipt_after_lease_lost");
    assert_eq!(late["stored"], json!(true));
    assert_eq!(late["token"], json!("tok-late-1"));
    assert_eq!(late["status"], json!("SUBMITTED"));
    assert_eq!(late["existing_token"], Value::Null);
    assert_eq!(late["owner"], json!(lease.owner));
    assert_eq!(late["channel"], json!("primary"));
    assert_eq!(count_events(&events, "submitted"), 0);

    // A second, different receipt is only logged; the first token is kept.
    let write = ctx.db.record_submit_receipt(
        &lease,
        t1 + Duration::seconds(1),
        "tok-late-2",
        next_poll,
        SubmitChannel::Fallback,
    )?;
    assert_eq!(write, ReceiptWrite::Logged);
    let after = ctx.job(&job.id)?;
    assert_eq!(after.token.as_deref(), Some("tok-late-1"));
    assert_eq!(after.status, JobStatus::Submitted);
    assert_eq!(after.submit_stage, Some(SubmitStage::Uncertain));
    assert_eq!(after.last_error, stored.last_error);

    let events = ctx.events(&job.id)?;
    let late: Vec<&Value> = events
        .iter()
        .filter(|e| e.event_type == "submit_receipt_after_lease_lost")
        .map(|e| &e.payload)
        .collect();
    assert_eq!(late.len(), 2);
    assert_eq!(late[1]["stored"], json!(false));
    assert_eq!(late[1]["token"], json!("tok-late-2"));
    assert_eq!(late[1]["existing_token"], json!("tok-late-1"));
    assert_eq!(late[1]["channel"], json!("fallback"));

    // Already UNCERTAIN: recovery leaves it alone and nothing is resubmittable.
    let report = ctx.db.recover_expired_leases(&ctx.config.project_id, t1)?;
    assert_eq!(report.uncertain_submits, 0);
    assert_eq!(report.released_claims, 0);
    assert!(
        ctx.db
            .claim_job(
                &job.id,
                WorkKind::Submit,
                ClaimTiming::Now,
                t1,
                Duration::minutes(30)
            )?
            .is_none()
    );
    Ok(())
}

#[test]
fn receipt_after_recovery_marked_submit_uncertain_is_still_stored() -> Result<()> {
    let ctx = Ctx::new()?;
    let job = ctx.create_queued_job()?;
    let t0 = Utc::now();

    let mut lease = ctx
        .db
        .claim_job(
            &job.id,
            WorkKind::Submit,
            ClaimTiming::Now,
            t0,
            Duration::minutes(30),
        )?
        .context("claim")?;
    assert!(ctx.db.begin_submit_dispatch(
        &mut lease,
        SubmitChannel::Primary,
        t0,
        Duration::minutes(30)
    )?);

    let t1 = t0 + Duration::minutes(31);
    let report = ctx
        .other_handle()
        .recover_expired_leases(&ctx.config.project_id, t1)?;
    assert_eq!(report.uncertain_submits, 1);
    let recovered = ctx.job(&job.id)?;
    assert_eq!(recovered.status, JobStatus::Submitted);
    assert_eq!(recovered.submit_stage, Some(SubmitStage::Uncertain));
    assert_no_lease(&recovered);
    let unknown = only_event(&ctx.events(&job.id)?, "submit_outcome_unknown").clone();
    assert_eq!(unknown["source"], json!("lease_expired"));
    assert_eq!(unknown["previous_owner"], json!(lease.owner));

    let write = ctx.db.record_submit_receipt(
        &lease,
        t1 + Duration::seconds(1),
        "tok-after-recovery",
        t1 + Duration::minutes(10),
        SubmitChannel::Primary,
    )?;
    assert_eq!(write, ReceiptWrite::StoredForRecovery);
    let stored = ctx.job(&job.id)?;
    assert_eq!(stored.status, JobStatus::Submitted);
    assert_eq!(stored.submit_stage, Some(SubmitStage::Uncertain));
    assert_eq!(stored.token.as_deref(), Some("tok-after-recovery"));
    assert_no_lease(&stored);
    assert!(
        stored
            .last_error
            .as_deref()
            .is_some_and(|e| e.contains("a receipt token is saved")),
        "last_error should point at the saved token: {:?}",
        stored.last_error
    );
    let events = ctx.events(&job.id)?;
    assert_eq!(
        only_event(&events, "submit_receipt_after_lease_lost")["stored"],
        json!(true)
    );
    assert_eq!(count_events(&events, "submitted"), 0);
    Ok(())
}

#[test]
fn forced_retry_revokes_dispatched_lease_and_late_receipt_is_only_logged() -> Result<()> {
    let ctx = Ctx::new()?;
    let job = ctx.create_queued_job()?;
    let t0 = Utc::now();

    let mut lease = ctx
        .db
        .claim_job(
            &job.id,
            WorkKind::Submit,
            ClaimTiming::Now,
            t0,
            Duration::minutes(30),
        )?
        .context("claim")?;
    assert!(ctx.db.begin_submit_dispatch(
        &mut lease,
        SubmitChannel::Primary,
        t0,
        Duration::minutes(30)
    )?);

    // `retry --force` style override while the request is in flight.
    ctx.other_handle().update_job_state_unchecked(
        &job.id,
        JobStatus::Queued,
        Some(0),
        Some(None),
        Some(None),
    )?;
    let requeued = ctx.job(&job.id)?;
    assert_eq!(requeued.status, JobStatus::Queued);
    assert_eq!(requeued.submit_stage, None);
    assert_no_lease(&requeued);

    let write = ctx.db.record_submit_receipt(
        &lease,
        t0 + Duration::seconds(5),
        "tok-after-override",
        t0 + Duration::minutes(10),
        SubmitChannel::Primary,
    )?;
    assert_eq!(write, ReceiptWrite::Logged);
    let after = ctx.job(&job.id)?;
    assert_eq!(after.status, JobStatus::Queued);
    assert_eq!(after.token, None);
    let events = ctx.events(&job.id)?;
    let late = only_event(&events, "submit_receipt_after_lease_lost");
    assert_eq!(late["stored"], json!(false));
    assert_eq!(late["token"], json!("tok-after-override"));
    assert_eq!(late["status"], json!("QUEUED"));
    assert_eq!(count_events(&events, "submitted"), 0);
    Ok(())
}

#[test]
fn imported_token_mid_flight_wins_and_worker_receipt_is_only_logged() -> Result<()> {
    let ctx = Ctx::new()?;
    let job = ctx.create_queued_job()?;
    let t0 = Utc::now();

    let mut lease = ctx
        .db
        .claim_job(
            &job.id,
            WorkKind::Submit,
            ClaimTiming::Now,
            t0,
            Duration::minutes(30),
        )?
        .context("claim")?;
    assert!(ctx.db.begin_submit_dispatch(
        &mut lease,
        SubmitChannel::Primary,
        t0,
        Duration::minutes(30)
    )?);

    // Email ingestion / `import-token` binds a token while the worker still waits.
    let email_poll_at = t0 + Duration::minutes(10);
    ctx.other_handle()
        .attach_token_to_job(&job.id, "tok-email", email_poll_at)?;
    let imported = ctx.job(&job.id)?;
    assert_eq!(imported.status, JobStatus::Processing);
    assert_eq!(imported.token.as_deref(), Some("tok-email"));
    assert_eq!(imported.submit_stage, None);
    assert_no_lease(&imported);

    let write = ctx.db.record_submit_receipt(
        &lease,
        t0 + Duration::seconds(5),
        "tok-worker",
        t0 + Duration::minutes(20),
        SubmitChannel::Primary,
    )?;
    assert_eq!(write, ReceiptWrite::Logged);
    let after = ctx.job(&job.id)?;
    assert_eq!(execution_state(&after), execution_state(&imported));
    assert_eq!(after.next_poll_at, Some(email_poll_at));

    assert_eq!(
        ctx.db.finish_lease(
            &lease,
            t0 + Duration::seconds(6),
            &change(JobStatus::Failed, None, Some("x")),
            "submit_failed",
            json!({}),
        )?,
        LeaseWrite::Lost(Some(JobStatus::Processing))
    );
    let events = ctx.events(&job.id)?;
    let late = only_event(&events, "submit_receipt_after_lease_lost");
    assert_eq!(late["stored"], json!(false));
    assert_eq!(late["token"], json!("tok-worker"));
    assert_eq!(late["existing_token"], json!("tok-email"));
    assert_eq!(late["status"], json!("PROCESSING"));
    assert_eq!(count_events(&events, "submitted"), 0);
    Ok(())
}

// ---------------------------------------------------------------------------
// (3) Cancel racing completion stays consistent.
// ---------------------------------------------------------------------------

#[test]
fn cancel_during_dispatch_then_late_receipt_keeps_cancel_and_stores_token() -> Result<()> {
    let ctx = Ctx::new()?;
    let job = ctx.create_queued_job()?;
    let t0 = Utc::now();

    let mut lease = ctx
        .db
        .claim_job(
            &job.id,
            WorkKind::Submit,
            ClaimTiming::Now,
            t0,
            Duration::minutes(30),
        )?
        .context("claim")?;
    assert!(ctx.db.begin_submit_dispatch(
        &mut lease,
        SubmitChannel::Primary,
        t0,
        Duration::minutes(30)
    )?);

    let outcome =
        ctx.other_handle()
            .cancel_job(&job.id, Some("user abort"), t0 + Duration::seconds(1))?;
    assert_eq!(
        outcome,
        CancelOutcome::Cancelled {
            previous_status: JobStatus::Submitted,
            previous_stage: Some(SubmitStage::Dispatched),
            lease_was_active: true,
        }
    );
    let cancelled = ctx.job(&job.id)?;
    assert_eq!(cancelled.status, JobStatus::Failed);
    assert_eq!(
        cancelled.last_error.as_deref(),
        Some("cancelled by user: user abort")
    );
    assert_eq!(cancelled.submit_stage, None);
    assert_eq!(cancelled.next_poll_at, None);
    assert_no_lease(&cancelled);

    let events = ctx.events(&job.id)?;
    let cancel_event = only_event(&events, "cancelled");
    assert_eq!(cancel_event["reason"], json!("user abort"));
    assert_eq!(cancel_event["previous_status"], json!("SUBMITTED"));
    assert_eq!(cancel_event["previous_submit_stage"], json!("DISPATCHED"));
    assert_eq!(cancel_event["lease_was_active"], json!(true));

    // The provider answers after the cancel, well within the lease's original TTL.
    let write = ctx.db.record_submit_receipt(
        &lease,
        t0 + Duration::seconds(2),
        "tok-after-cancel",
        t0 + Duration::minutes(10),
        SubmitChannel::Primary,
    )?;
    assert_eq!(write, ReceiptWrite::StoredForRecovery);
    let after = ctx.job(&job.id)?;
    assert_eq!(after.status, JobStatus::Failed, "cancel must stick");
    assert_eq!(
        after.last_error.as_deref(),
        Some("cancelled by user: user abort")
    );
    assert_eq!(after.token.as_deref(), Some("tok-after-cancel"));
    assert_eq!(after.submit_stage, None);
    assert_eq!(after.next_poll_at, None);
    assert_no_lease(&after);

    // The worker's own finish is rejected too.
    let write = ctx.db.finish_lease(
        &lease,
        t0 + Duration::seconds(3),
        &change(
            JobStatus::Submitted,
            Some(SubmitStage::Uncertain),
            Some("outcome unknown"),
        ),
        "submit_outcome_unknown",
        json!({}),
    )?;
    assert_eq!(write, LeaseWrite::Lost(Some(JobStatus::Failed)));
    assert_eq!(execution_state(&ctx.job(&job.id)?), execution_state(&after));

    let events = ctx.events(&job.id)?;
    let late = only_event(&events, "submit_receipt_after_lease_lost");
    assert_eq!(late["stored"], json!(true));
    assert_eq!(late["status"], json!("FAILED"));
    assert_eq!(late["token"], json!("tok-after-cancel"));
    assert_eq!(count_events(&events, "submitted"), 0);
    assert_eq!(count_events(&events, "submit_outcome_unknown"), 0);
    Ok(())
}

#[test]
fn completed_job_cannot_be_cancelled_afterwards() -> Result<()> {
    let ctx = Ctx::new()?;
    let t0 = Utc::now();
    let job = ctx.create_processing_job("tok-done", t0)?;

    let lease = ctx
        .db
        .claim_job(
            &job.id,
            WorkKind::Poll,
            ClaimTiming::Now,
            t0,
            Duration::minutes(10),
        )?
        .context("claim")?;
    let write = ctx.db.finish_lease_with_review(
        &lease,
        t0 + Duration::seconds(1),
        review("tok-done", r#"{"ok":true}"#),
        &completed_change(),
        "review_completed",
        json!({ "token": "tok-done" }),
    )?;
    assert_eq!(write, LeaseWrite::Applied);

    let outcome =
        ctx.other_handle()
            .cancel_job(&job.id, Some("too late"), t0 + Duration::seconds(2))?;
    assert_eq!(
        outcome,
        CancelOutcome::AlreadyTerminal(JobStatus::Completed)
    );
    let after = ctx.job(&job.id)?;
    assert_eq!(after.status, JobStatus::Completed);
    assert_eq!(after.last_error, None);
    assert_no_lease(&after);
    assert_eq!(ctx.review_count(&job.id)?, 1);
    let events = ctx.events(&job.id)?;
    assert_eq!(count_events(&events, "cancelled"), 0);
    assert_eq!(count_events(&events, "review_completed"), 1);
    Ok(())
}

#[test]
fn cancel_before_poll_finish_rejects_the_review() -> Result<()> {
    let ctx = Ctx::new()?;
    let t0 = Utc::now();
    let job = ctx.create_processing_job("tok-cancel-first", t0)?;

    let lease = ctx
        .db
        .claim_job(
            &job.id,
            WorkKind::Poll,
            ClaimTiming::Now,
            t0,
            Duration::minutes(10),
        )?
        .context("claim")?;
    let outcome = ctx.other_handle().cancel_job(&job.id, None, t0)?;
    assert_eq!(
        outcome,
        CancelOutcome::Cancelled {
            previous_status: JobStatus::Processing,
            previous_stage: None,
            lease_was_active: true,
        }
    );

    let write = ctx.db.finish_lease_with_review(
        &lease,
        t0 + Duration::seconds(1),
        review("tok-cancel-first", r#"{"ok":true}"#),
        &completed_change(),
        "review_completed",
        json!({}),
    )?;
    assert_eq!(write, LeaseWrite::Lost(Some(JobStatus::Failed)));
    let after = ctx.job(&job.id)?;
    assert_eq!(after.status, JobStatus::Failed);
    assert_eq!(after.last_error.as_deref(), Some("cancelled by user"));
    assert_eq!(after.token.as_deref(), Some("tok-cancel-first"));
    assert_no_lease(&after);
    assert_eq!(ctx.review_count(&job.id)?, 0);
    let events = ctx.events(&job.id)?;
    assert_eq!(count_events(&events, "review_completed"), 0);
    assert_eq!(only_event(&events, "cancelled")["reason"], Value::Null);
    Ok(())
}

/// Run `left` and `right` on two threads, each with its own `Db` handle, released
/// together by a barrier.
fn race<L, R, A, B>(path: &Path, left: L, right: R) -> (A, B)
where
    L: FnOnce(&Db) -> A + Send,
    R: FnOnce(&Db) -> B + Send,
    A: Send,
    B: Send,
{
    let barrier = Barrier::new(2);
    std::thread::scope(|scope| {
        let left = scope.spawn(|| {
            let db = Db::new_file(path.to_path_buf());
            barrier.wait();
            left(&db)
        });
        let right = scope.spawn(|| {
            let db = Db::new_file(path.to_path_buf());
            barrier.wait();
            right(&db)
        });
        (
            left.join().expect("left racer panicked"),
            right.join().expect("right racer panicked"),
        )
    })
}

#[test]
fn cancel_racing_poll_completion_has_exactly_one_winner() -> Result<()> {
    let ctx = Ctx::new()?;
    let (mut cancel_won, mut complete_won) = (0usize, 0usize);

    for round in 0..RACE_ROUNDS {
        let token = format!("tok-race-{round}");
        let job = ctx.create_processing_job(&token, Utc::now())?;
        let lease = ctx
            .db
            .claim_job(
                &job.id,
                WorkKind::Poll,
                ClaimTiming::Now,
                Utc::now(),
                Duration::minutes(10),
            )?
            .context("claim")?;

        let (cancel, finish) = race(
            &ctx.db.path,
            |db| db.cancel_job(&job.id, Some("race"), Utc::now()),
            |db| {
                db.finish_lease_with_review(
                    &lease,
                    Utc::now(),
                    NewReview {
                        token: &token,
                        raw_json: r#"{"race":true}"#,
                        summary_md: "# race",
                    },
                    &completed_change(),
                    "review_completed",
                    json!({ "token": token }),
                )
            },
        );
        let (cancel, finish) = (cancel?, finish?);

        let after = ctx.job(&job.id)?;
        let events = ctx.events(&job.id)?;
        let reviews = ctx.review_count(&job.id)?;
        assert_no_lease(&after);
        match (cancel, finish) {
            (CancelOutcome::AlreadyTerminal(JobStatus::Completed), LeaseWrite::Applied) => {
                complete_won += 1;
                assert_eq!(after.status, JobStatus::Completed, "round {round}");
                assert_eq!(after.last_error, None, "round {round}");
                assert_eq!(reviews, 1, "round {round}");
                assert_eq!(
                    count_events(&events, "review_completed"),
                    1,
                    "round {round}"
                );
                assert_eq!(count_events(&events, "cancelled"), 0, "round {round}");
            }
            (
                CancelOutcome::Cancelled {
                    previous_status: JobStatus::Processing,
                    previous_stage: None,
                    lease_was_active: true,
                },
                LeaseWrite::Lost(Some(JobStatus::Failed)),
            ) => {
                cancel_won += 1;
                assert_eq!(after.status, JobStatus::Failed, "round {round}");
                assert_eq!(
                    after.last_error.as_deref(),
                    Some("cancelled by user: race"),
                    "round {round}"
                );
                assert_eq!(reviews, 0, "round {round}");
                assert_eq!(
                    count_events(&events, "review_completed"),
                    0,
                    "round {round}"
                );
                assert_eq!(count_events(&events, "cancelled"), 1, "round {round}");
            }
            other => panic!("round {round}: inconsistent race outcome {other:?}, job {after:?}"),
        }
    }

    assert_eq!(cancel_won + complete_won, RACE_ROUNDS);
    eprintln!("cancel vs completion: cancel won {cancel_won}, completion won {complete_won}");
    Ok(())
}

#[test]
fn cancel_racing_dispatch_never_leaves_a_dispatch_after_cancel() -> Result<()> {
    let ctx = Ctx::new()?;
    let (mut cancel_won, mut dispatch_won) = (0usize, 0usize);

    for round in 0..RACE_ROUNDS {
        let job = ctx.create_queued_job()?;
        let lease = ctx
            .db
            .claim_job(
                &job.id,
                WorkKind::Submit,
                ClaimTiming::Now,
                Utc::now(),
                Duration::minutes(30),
            )?
            .context("claim")?;

        let (cancel, dispatch) = race(
            &ctx.db.path,
            |db| db.cancel_job(&job.id, Some("race"), Utc::now()),
            |db| {
                let mut lease = lease.clone();
                db.begin_submit_dispatch(
                    &mut lease,
                    SubmitChannel::Primary,
                    Utc::now(),
                    Duration::minutes(30),
                )
            },
        );
        let (cancel, dispatched) = (cancel?, dispatch?);

        let after = ctx.job(&job.id)?;
        let events = ctx.events(&job.id)?;
        assert_eq!(after.status, JobStatus::Failed, "round {round}");
        assert_eq!(
            after.last_error.as_deref(),
            Some("cancelled by user: race"),
            "round {round}"
        );
        assert_eq!(after.submit_stage, None, "round {round}");
        assert_no_lease(&after);
        let expected_previous = if dispatched {
            dispatch_won += 1;
            (JobStatus::Submitted, Some(SubmitStage::Dispatched))
        } else {
            cancel_won += 1;
            (JobStatus::Queued, Some(SubmitStage::Claimed))
        };
        assert_eq!(
            cancel,
            CancelOutcome::Cancelled {
                previous_status: expected_previous.0,
                previous_stage: expected_previous.1,
                lease_was_active: true,
            },
            "round {round}"
        );
        assert_eq!(
            count_events(&events, "submit_dispatched"),
            usize::from(dispatched),
            "round {round}"
        );
        assert_eq!(count_events(&events, "cancelled"), 1, "round {round}");
    }

    eprintln!("cancel vs dispatch: cancel won {cancel_won}, dispatch won {dispatch_won}");
    Ok(())
}

#[test]
fn cancel_racing_submit_receipt_always_ends_cancelled_with_token_kept() -> Result<()> {
    let ctx = Ctx::new()?;
    let (mut cancel_won, mut receipt_won) = (0usize, 0usize);

    for round in 0..RACE_ROUNDS {
        let token = format!("tok-receipt-race-{round}");
        let job = ctx.create_queued_job()?;
        let mut lease = ctx
            .db
            .claim_job(
                &job.id,
                WorkKind::Submit,
                ClaimTiming::Now,
                Utc::now(),
                Duration::minutes(30),
            )?
            .context("claim")?;
        assert!(ctx.db.begin_submit_dispatch(
            &mut lease,
            SubmitChannel::Primary,
            Utc::now(),
            Duration::minutes(30)
        )?);

        let (cancel, receipt) = race(
            &ctx.db.path,
            |db| db.cancel_job(&job.id, Some("race"), Utc::now()),
            |db| {
                db.record_submit_receipt(
                    &lease,
                    Utc::now(),
                    &token,
                    Utc::now() + Duration::minutes(10),
                    SubmitChannel::Primary,
                )
            },
        );
        let (cancel, receipt) = (cancel?, receipt?);

        let after = ctx.job(&job.id)?;
        let events = ctx.events(&job.id)?;
        assert_eq!(after.status, JobStatus::Failed, "round {round}");
        assert_eq!(
            after.last_error.as_deref(),
            Some("cancelled by user: race"),
            "round {round}"
        );
        assert_eq!(
            after.token.as_deref(),
            Some(token.as_str()),
            "round {round}"
        );
        assert_eq!(after.submit_stage, None, "round {round}");
        assert_eq!(after.next_poll_at, None, "round {round}");
        assert_no_lease(&after);
        match (cancel, receipt) {
            (
                CancelOutcome::Cancelled {
                    previous_status: JobStatus::Processing,
                    previous_stage: None,
                    lease_was_active: false,
                },
                ReceiptWrite::Accepted,
            ) => {
                receipt_won += 1;
                assert_eq!(count_events(&events, "submitted"), 1, "round {round}");
                assert_eq!(
                    count_events(&events, "submit_receipt_after_lease_lost"),
                    0,
                    "round {round}"
                );
            }
            (
                CancelOutcome::Cancelled {
                    previous_status: JobStatus::Submitted,
                    previous_stage: Some(SubmitStage::Dispatched),
                    lease_was_active: true,
                },
                ReceiptWrite::StoredForRecovery,
            ) => {
                cancel_won += 1;
                assert_eq!(count_events(&events, "submitted"), 0, "round {round}");
                assert_eq!(
                    only_event(&events, "submit_receipt_after_lease_lost")["stored"],
                    json!(true),
                    "round {round}"
                );
            }
            other => panic!("round {round}: inconsistent race outcome {other:?}, job {after:?}"),
        }
        assert_eq!(count_events(&events, "cancelled"), 1, "round {round}");
    }

    eprintln!("cancel vs receipt: cancel won {cancel_won}, receipt won {receipt_won}");
    Ok(())
}

// ---------------------------------------------------------------------------
// Worker level: a mock backend held mid-call while another handle intervenes.
// ---------------------------------------------------------------------------

/// Backend whose submits and fetches signal `entered` and then block until `release`
/// fires; a submit then answers with `token`, a fetch with a ready review.
fn gated_backend(token: &'static str) -> MockBackend {
    MockBackend::default()
        .on_submit(move || Answer::OnRelease(Ok(receipt(token))))
        .on_fetch(|| Answer::OnRelease(Ok(ready_review())))
}

#[tokio::test]
async fn worker_submit_cancelled_mid_flight_keeps_cancel_and_stores_late_receipt() -> Result<()> {
    let ctx = Ctx::new()?;
    let job = ctx.create_queued_job()?;
    let backend = gated_backend("tok-worker-cancel");
    let other = ctx.other_handle();

    let (attempt, cancel) = with_deadline(async {
        tokio::join!(
            worker::submit_job_with_backend(&ctx.config, &ctx.db, &job.id, &backend),
            async {
                backend.entered.notified().await;
                let in_flight = other.get_job(&job.id)?.context("job")?;
                assert_eq!(in_flight.status, JobStatus::Submitted);
                assert_eq!(in_flight.submit_stage, Some(SubmitStage::Dispatched));
                assert!(in_flight.lease_owner.is_some());
                let outcome = other.cancel_job(&job.id, Some("operator abort"), Utc::now());
                backend.release.notify_one();
                outcome
            }
        )
    })
    .await?;

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

    let after = ctx.job(&job.id)?;
    assert_eq!(after.status, JobStatus::Failed);
    assert_eq!(
        after.last_error.as_deref(),
        Some("cancelled by user: operator abort")
    );
    assert_eq!(after.token.as_deref(), Some("tok-worker-cancel"));
    assert_eq!(after.submit_stage, None);
    assert_eq!(after.next_poll_at, None);
    assert_no_lease(&after);

    let events = ctx.events(&job.id)?;
    let types = event_types(&events);
    assert_eq!(
        types,
        vec![
            "submit_dispatched",
            "cancelled",
            "submit_receipt_after_lease_lost"
        ]
    );
    let late = only_event(&events, "submit_receipt_after_lease_lost");
    assert_eq!(late["stored"], json!(true));
    assert_eq!(late["token"], json!("tok-worker-cancel"));
    assert_eq!(late["status"], json!("FAILED"));
    assert_eq!(count_events(&events, "submitted"), 0);

    // Nothing is resubmitted afterwards.
    assert_eq!(
        worker::submit_job_with_backend(&ctx.config, &ctx.db, &job.id, &backend).await?,
        Attempt::NotClaimed
    );
    assert_eq!(backend.submit_count(), 1);
    Ok(())
}

#[tokio::test]
async fn worker_submit_whose_lease_expires_mid_flight_keeps_receipt_for_recovery() -> Result<()> {
    let ctx = Ctx::new()?;
    let job = ctx.create_queued_job()?;
    let backend = gated_backend("tok-worker-stalled");
    let other = ctx.other_handle();
    let project_id = ctx.config.project_id.clone();

    let (attempt, recovery) = with_deadline(async {
        tokio::join!(
            worker::submit_job_with_backend(&ctx.config, &ctx.db, &job.id, &backend),
            async {
                backend.entered.notified().await;
                // Another tick, 31 minutes later, finds the submit lease expired.
                let report =
                    other.recover_expired_leases(&project_id, Utc::now() + Duration::minutes(31));
                backend.release.notify_one();
                report
            }
        )
    })
    .await?;

    assert_eq!(attempt?, Attempt::Ran);
    let recovery = recovery?;
    assert_eq!(recovery.uncertain_submits, 1);
    assert_eq!(recovery.released_claims, 0);
    assert_eq!(backend.submit_count(), 1);

    let after = ctx.job(&job.id)?;
    assert_eq!(after.status, JobStatus::Submitted);
    assert_eq!(after.submit_stage, Some(SubmitStage::Uncertain));
    assert_eq!(after.token.as_deref(), Some("tok-worker-stalled"));
    assert_no_lease(&after);
    assert!(
        after
            .last_error
            .as_deref()
            .is_some_and(|e| e.contains("a receipt token is saved")),
        "unexpected last_error: {:?}",
        after.last_error
    );

    let events = ctx.events(&job.id)?;
    assert_eq!(
        event_types(&events),
        vec![
            "submit_dispatched",
            "submit_outcome_unknown",
            "submit_receipt_after_lease_lost"
        ]
    );
    assert_eq!(
        only_event(&events, "submit_outcome_unknown")["source"],
        json!("lease_expired")
    );
    assert_eq!(
        only_event(&events, "submit_receipt_after_lease_lost")["stored"],
        json!(true)
    );

    // Pending reconciliation: no automatic resubmission.
    assert_eq!(
        worker::submit_job_with_backend(&ctx.config, &ctx.db, &job.id, &backend).await?,
        Attempt::NotClaimed
    );
    assert_eq!(backend.submit_count(), 1);
    Ok(())
}

#[tokio::test]
async fn worker_poll_taken_over_mid_flight_rejects_stale_review() -> Result<()> {
    let ctx = Ctx::new()?;
    let job = ctx.create_processing_job("tok-worker-poll", Utc::now())?;
    let backend = gated_backend("unused");
    let other = ctx.other_handle();
    let takeover_at = Utc::now() + Duration::minutes(11);

    let (attempt, lease_b) = with_deadline(async {
        tokio::join!(
            worker::poll_job_with_backend(&ctx.config, &ctx.db, &job.id, &backend),
            async {
                backend.entered.notified().await;
                // The worker stalls past its 10-minute poll lease; another worker claims.
                let lease = other.claim_job(
                    &job.id,
                    WorkKind::Poll,
                    ClaimTiming::Now,
                    takeover_at,
                    Duration::minutes(10),
                );
                backend.release.notify_one();
                lease
            }
        )
    })
    .await?;

    assert_eq!(attempt?, Attempt::Ran);
    let lease_b = lease_b?.context("B must take over the expired poll lease")?;
    assert_eq!(backend.fetch_count(), 1);

    let after = ctx.job(&job.id)?;
    assert_eq!(after.status, JobStatus::Processing);
    assert_eq!(after.attempt, 0, "stale owner must not bump attempt");
    assert_held_by(&after, &lease_b);
    assert_eq!(ctx.review_count(&job.id)?, 0);

    let events = ctx.events(&job.id)?;
    assert_eq!(count_events(&events, "review_completed"), 0);
    let rejected = only_event(&events, "stale_result_rejected");
    assert_eq!(rejected["kind"], json!("poll"));
    assert_eq!(rejected["outcome"], json!("review_completed"));
    assert_eq!(rejected["current_status"], json!("PROCESSING"));
    assert_ne!(rejected["owner"], json!(lease_b.owner));

    // B's own result applies.
    let write = other.finish_lease_with_review(
        &lease_b,
        takeover_at + Duration::minutes(1),
        review("tok-worker-poll", r#"{"from":"B"}"#),
        &completed_change(),
        "review_completed",
        json!({ "token": "tok-worker-poll" }),
    )?;
    assert_eq!(write, LeaseWrite::Applied);
    let done = ctx.job(&job.id)?;
    assert_eq!(done.status, JobStatus::Completed);
    assert_no_lease(&done);
    assert_eq!(
        ctx.review_raw_json(&job.id)?.as_deref(),
        Some(r#"{"from":"B"}"#)
    );
    Ok(())
}

#[tokio::test]
async fn worker_poll_cancelled_mid_flight_rejects_review() -> Result<()> {
    let ctx = Ctx::new()?;
    let job = ctx.create_processing_job("tok-worker-poll-cancel", Utc::now())?;
    let backend = gated_backend("unused");
    let other = ctx.other_handle();

    let (attempt, cancel) = with_deadline(async {
        tokio::join!(
            worker::poll_job_with_backend(&ctx.config, &ctx.db, &job.id, &backend),
            async {
                backend.entered.notified().await;
                let outcome = other.cancel_job(&job.id, Some("stop"), Utc::now());
                backend.release.notify_one();
                outcome
            }
        )
    })
    .await?;

    assert_eq!(attempt?, Attempt::Ran);
    assert_eq!(
        cancel?,
        CancelOutcome::Cancelled {
            previous_status: JobStatus::Processing,
            previous_stage: None,
            lease_was_active: true,
        }
    );
    let after = ctx.job(&job.id)?;
    assert_eq!(after.status, JobStatus::Failed);
    assert_eq!(after.last_error.as_deref(), Some("cancelled by user: stop"));
    assert_no_lease(&after);
    assert_eq!(ctx.review_count(&job.id)?, 0);

    let events = ctx.events(&job.id)?;
    assert_eq!(count_events(&events, "review_completed"), 0);
    assert_eq!(count_events(&events, "cancelled"), 1);
    let rejected = only_event(&events, "stale_result_rejected");
    assert_eq!(rejected["current_status"], json!("FAILED"));
    assert_eq!(rejected["outcome"], json!("review_completed"));
    Ok(())
}

// ---------------------------------------------------------------------------
// Generic state writes and live leases.
// ---------------------------------------------------------------------------

#[test]
fn unchecked_update_revokes_a_live_lease() -> Result<()> {
    let ctx = Ctx::new()?;
    let t0 = Utc::now();
    let job = ctx.create_processing_job("tok-unchecked", t0)?;

    let lease = ctx
        .db
        .claim_job(
            &job.id,
            WorkKind::Poll,
            ClaimTiming::Now,
            t0,
            Duration::minutes(10),
        )?
        .context("claim")?;
    // Same status, but an override still revokes the lease.
    ctx.other_handle().update_job_state_unchecked(
        &job.id,
        JobStatus::Processing,
        Some(0),
        None,
        Some(None),
    )?;
    let overridden = ctx.job(&job.id)?;
    assert_eq!(overridden.status, JobStatus::Processing);
    assert_no_lease(&overridden);

    let write = ctx.db.finish_lease(
        &lease,
        t0 + Duration::seconds(1),
        &change(JobStatus::Failed, None, Some("invalid token")),
        "invalid_token",
        json!({}),
    )?;
    assert_eq!(write, LeaseWrite::Lost(Some(JobStatus::Processing)));
    let after = ctx.job(&job.id)?;
    assert_eq!(execution_state(&after), execution_state(&overridden));
    assert_eq!(count_events(&ctx.events(&job.id)?, "invalid_token"), 0);

    // A dispatched submit lease is revoked and its stage cleared as well.
    let submit_job = ctx.create_queued_job()?;
    let mut submit_lease = ctx
        .db
        .claim_job(
            &submit_job.id,
            WorkKind::Submit,
            ClaimTiming::Now,
            t0,
            Duration::minutes(30),
        )?
        .context("claim submit")?;
    assert!(ctx.db.begin_submit_dispatch(
        &mut submit_lease,
        SubmitChannel::Primary,
        t0,
        Duration::minutes(30)
    )?);
    ctx.other_handle().update_job_state_unchecked(
        &submit_job.id,
        JobStatus::Submitted,
        None,
        None,
        None,
    )?;
    let revoked = ctx.job(&submit_job.id)?;
    assert_eq!(revoked.status, JobStatus::Submitted);
    assert_eq!(revoked.submit_stage, None);
    assert_no_lease(&revoked);
    assert_eq!(
        ctx.db.finish_lease(
            &submit_lease,
            t0 + Duration::seconds(1),
            &change(JobStatus::Failed, None, Some("x")),
            "submit_failed",
            json!({}),
        )?,
        LeaseWrite::Lost(Some(JobStatus::Submitted))
    );
    Ok(())
}

#[test]
fn checked_update_keeps_lease_on_same_status_and_revokes_it_on_status_change() -> Result<()> {
    let ctx = Ctx::new()?;
    let t0 = Utc::now();

    // Same-status bookkeeping keeps the owner's lease.
    let job = ctx.create_processing_job("tok-checked", t0)?;
    let lease = ctx
        .db
        .claim_job(
            &job.id,
            WorkKind::Poll,
            ClaimTiming::Now,
            t0,
            Duration::minutes(10),
        )?
        .context("claim")?;
    ctx.other_handle().update_job_state(
        &job.id,
        JobStatus::Processing,
        Some(5),
        None,
        Some(Some("bookkeeping".to_string())),
    )?;
    let bumped = ctx.job(&job.id)?;
    assert_eq!(bumped.attempt, 5);
    assert_eq!(bumped.last_error.as_deref(), Some("bookkeeping"));
    assert_held_by(&bumped, &lease);

    let write = ctx.db.finish_lease(
        &lease,
        t0 + Duration::seconds(1),
        &JobChange {
            status: JobStatus::Processing,
            attempt: Some(6),
            next_poll_at: Some(Some(t0 + Duration::minutes(20))),
            last_error: Some(None),
            submit_stage: None,
            fallback_used: None,
        },
        "poll_processing",
        json!({ "attempt": 6 }),
    )?;
    assert_eq!(write, LeaseWrite::Applied);
    let polled = ctx.job(&job.id)?;
    assert_eq!(polled.attempt, 6);
    assert_eq!(polled.last_error, None);
    assert_no_lease(&polled);
    assert_eq!(count_events(&ctx.events(&job.id)?, "poll_processing"), 1);

    // Same-status bookkeeping on a CLAIMED submit keeps both lease and stage.
    let submit_job = ctx.create_queued_job()?;
    let submit_lease = ctx
        .db
        .claim_job(
            &submit_job.id,
            WorkKind::Submit,
            ClaimTiming::Now,
            t0,
            Duration::minutes(30),
        )?
        .context("claim submit")?;
    ctx.other_handle()
        .update_job_state(&submit_job.id, JobStatus::Queued, Some(2), None, None)?;
    let kept = ctx.job(&submit_job.id)?;
    assert_eq!(kept.submit_stage, Some(SubmitStage::Claimed));
    assert_held_by(&kept, &submit_lease);

    // A status change revokes the lease.
    let job2 = ctx.create_processing_job("tok-checked-2", t0)?;
    let lease2 = ctx
        .db
        .claim_job(
            &job2.id,
            WorkKind::Poll,
            ClaimTiming::Now,
            t0,
            Duration::minutes(10),
        )?
        .context("claim")?;
    ctx.other_handle()
        .update_job_state(&job2.id, JobStatus::Queued, None, Some(None), None)?;
    let requeued = ctx.job(&job2.id)?;
    assert_eq!(requeued.status, JobStatus::Queued);
    assert_no_lease(&requeued);
    assert_eq!(
        ctx.db.finish_lease(
            &lease2,
            t0 + Duration::seconds(1),
            &completed_change(),
            "review_completed",
            json!({}),
        )?,
        LeaseWrite::Lost(Some(JobStatus::Queued))
    );
    assert_eq!(ctx.job(&job2.id)?.status, JobStatus::Queued);
    Ok(())
}
