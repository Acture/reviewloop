//! Regression tests pinning the review fixes in fd2ba2b (OSS-337), against a file-backed
//! SQLite database:
//! 1. `Db::reschedule` keeps status, lease and stage, so a sibling status the CLI read
//!    before a worker dispatched it can no longer become a transition that revokes the
//!    in-flight submission.
//! 2. `Db::pull_poll_forward` only moves a PROCESSING job's next poll earlier.
//! 3. `Db::requeue` refuses a submission in flight and otherwise resets the row.
//! 4. A missing fallback script is a definitive failure that clears `fallback_used`, so
//!    a retry dispatches the fallback again.
//! 5. A receipt stored for recovery sets `started_at`.
//!
//! Database primitives run on a fixed simulated clock far from the wall clock, so a
//! column written from the passed `now` is distinguishable from one written from
//! `Utc::now()`.

mod common;

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use common::{
    Answer, Ctx, MockBackend, PAPER, POLL_TTL, PROJECT, SUBMIT_TTL, assert_held_by,
    assert_no_lease, dispatch_channels,
};
use reviewloop::{
    backend::BackendError,
    db::{
        CancelOutcome, ClaimTiming, Db, JobChange, Lease, LeaseRecovery, LeaseWrite, ReceiptWrite,
        Requeue,
    },
    model::{Job, JobStatus, SubmitChannel, SubmitStage, WorkKind},
    worker::{self, Attempt},
};
use serde_json::{Value, json};

const REJECTION: &str = "upload rejected";
const BACKOFF_ERROR: &str = "rate limited: slow down";

impl Ctx {
    /// A QUEUED job with retry bookkeeping to reset: attempt 3, a cooldown until
    /// `now + 10m` and a rate-limit diagnostic.
    fn create_backed_off_job(&self, now: DateTime<Utc>) -> Result<Job> {
        let job = self.create_queued_job()?;
        self.db.update_job_state(
            &job.id,
            JobStatus::Queued,
            Some(3),
            Some(Some(now + Duration::minutes(10))),
            Some(Some(BACKOFF_ERROR.to_string())),
        )?;
        let backed_off = self.job(&job.id)?;
        assert_eq!(backed_off.attempt, 3);
        assert_eq!(backed_off.next_poll_at, Some(now + Duration::minutes(10)));
        assert_eq!(backed_off.last_error.as_deref(), Some(BACKOFF_ERROR));
        Ok(backed_off)
    }
}

/// The execution-state columns of a job, compared as a whole to show a call left the
/// row alone (`Job` has no `PartialEq`, and `updated_at` is wall-clock).
#[derive(Debug, PartialEq)]
struct RowState {
    status: JobStatus,
    submit_stage: Option<SubmitStage>,
    lease_owner: Option<String>,
    lease_expires_at: Option<DateTime<Utc>>,
    attempt: u32,
    next_poll_at: Option<DateTime<Utc>>,
    last_error: Option<String>,
    fallback_used: bool,
    token: Option<String>,
    started_at: Option<DateTime<Utc>>,
}

impl From<&Job> for RowState {
    fn from(job: &Job) -> Self {
        Self {
            status: job.status,
            submit_stage: job.submit_stage,
            lease_owner: job.lease_owner.clone(),
            lease_expires_at: job.lease_expires_at,
            attempt: job.attempt,
            next_poll_at: job.next_poll_at,
            last_error: job.last_error.clone(),
            fallback_used: job.fallback_used,
            token: job.token.clone(),
            started_at: job.started_at,
        }
    }
}

/// Fixed simulated clock, far from the wall clock.
fn t0() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2030-01-01T00:00:00Z")
        .expect("valid timestamp")
        .with_timezone(&Utc)
}

fn claim_submit(db: &Db, job_id: &str, now: DateTime<Utc>) -> Result<Lease> {
    db.claim_job(job_id, WorkKind::Submit, ClaimTiming::Now, now, SUBMIT_TTL)?
        .context("submit claim must succeed")
}

/// Claim the job for submit and record a primary dispatch, as the worker does right
/// before sending.
fn dispatch(db: &Db, job_id: &str, now: DateTime<Utc>) -> Result<Lease> {
    let mut lease = claim_submit(db, job_id, now)?;
    assert!(
        db.begin_submit_dispatch(&mut lease, SubmitChannel::Primary, now, SUBMIT_TTL)?,
        "the claim owner must be able to dispatch"
    );
    assert_eq!(lease.expires_at, now + SUBMIT_TTL);
    Ok(lease)
}

/// `requeue` left a fresh QUEUED row that the daemon picks up at `now`.
fn assert_requeued(ctx: &Ctx, job_id: &str, now: DateTime<Utc>) -> Result<Job> {
    let job = ctx.job(job_id)?;
    assert_eq!(job.status, JobStatus::Queued);
    assert_eq!(job.attempt, 0);
    assert_eq!(job.next_poll_at, None);
    assert_eq!(job.last_error, None);
    assert_eq!(job.submit_stage, None);
    assert_no_lease(&job);
    assert!(
        ctx.ready_ids(now)?.contains(&job.id),
        "a requeued job must be claimable by the daemon"
    );
    Ok(job)
}

// ---------------------------------------------------------------------------------------
// 1. reschedule keeps status, lease and stage
// ---------------------------------------------------------------------------------------

/// `submit --force` reads the paper's siblings, then resets their cooldowns. A worker
/// that dispatches a sibling in between must keep its submission and its receipt.
#[test]
fn reschedule_after_stale_queued_read_keeps_in_flight_dispatch() -> Result<()> {
    let ctx = Ctx::new()?;
    let now = t0();
    let new_job = ctx.create_queued_job()?;
    let sibling = ctx.create_backed_off_job(now)?;
    let worker_db = ctx.other_handle();

    // CLI handle: the sibling is read while still QUEUED.
    let stale: Vec<Job> = ctx
        .db
        .list_active_jobs_for_paper(PROJECT, PAPER)?
        .into_iter()
        .filter(|job| job.id != new_job.id)
        .collect();
    assert_eq!(stale.len(), 1);
    assert_eq!(stale[0].id, sibling.id);
    assert_eq!(stale[0].status, JobStatus::Queued);

    // Worker handle: claims and dispatches the sibling before the CLI writes.
    let lease = dispatch(&worker_db, &sibling.id, now)?;
    let dispatched = ctx.job(&sibling.id)?;
    assert_eq!(dispatched.status, JobStatus::Submitted);
    assert_eq!(dispatched.submit_stage, Some(SubmitStage::Dispatched));
    assert_held_by(&dispatched, &lease);

    // CLI handle: the cooldown reset from the stale read.
    ctx.db.reschedule(&stale[0].id, Some(0), Some(None))?;

    let after = ctx.job(&sibling.id)?;
    assert_eq!(after.status, JobStatus::Submitted);
    assert_eq!(after.submit_stage, Some(SubmitStage::Dispatched));
    assert_held_by(&after, &lease);
    assert_eq!(after.attempt, 0);
    assert_eq!(after.next_poll_at, None);
    assert_eq!(after.last_error.as_deref(), Some(BACKOFF_ERROR));
    assert_eq!(after.token, None);
    assert_eq!(ctx.event_types(&sibling.id)?, ["submit_dispatched"]);

    // The owner's receipt is still recorded.
    let next_poll = now + Duration::minutes(10);
    assert_eq!(
        worker_db.record_submit_receipt(
            &lease,
            now + Duration::minutes(1),
            "tok-sibling",
            next_poll,
            SubmitChannel::Primary,
        )?,
        ReceiptWrite::Accepted
    );
    let done = ctx.job(&sibling.id)?;
    assert_eq!(done.status, JobStatus::Processing);
    assert_eq!(done.token.as_deref(), Some("tok-sibling"));
    assert_eq!(done.submit_stage, None);
    assert_no_lease(&done);
    assert_eq!(done.attempt, 0);
    assert_eq!(done.next_poll_at, Some(next_poll));
    assert_eq!(done.last_error, None);
    let events = ctx.events(&sibling.id)?;
    assert_eq!(
        ctx.event_types(&sibling.id)?,
        ["submit_dispatched", "submitted"]
    );
    assert_eq!(events[0].payload["owner"], lease.owner.as_str());
    assert_eq!(events[1].payload["token"], "tok-sibling");
    assert_eq!(events[1].payload["channel"], "primary");

    // The new job was never touched.
    assert_eq!(
        RowState::from(&ctx.job(&new_job.id)?),
        RowState::from(&new_job)
    );
    Ok(())
}

/// Control for the test above: the pre-fix reset wrote the stale status back through
/// `update_job_state`, which turns the same race into SUBMITTED -> QUEUED and revokes the
/// in-flight lease, so the owner's receipt is only logged.
#[test]
fn control_stale_status_write_revokes_in_flight_dispatch() -> Result<()> {
    let ctx = Ctx::new()?;
    let now = t0();
    let sibling = ctx.create_backed_off_job(now)?;
    let worker_db = ctx.other_handle();

    let stale = ctx.job(&sibling.id)?;
    let lease = dispatch(&worker_db, &sibling.id, now)?;
    ctx.db
        .update_job_state(&stale.id, stale.status, Some(0), Some(None), None)?;

    let revoked = ctx.job(&sibling.id)?;
    assert_eq!(revoked.status, JobStatus::Queued);
    assert_eq!(revoked.submit_stage, None);
    assert_no_lease(&revoked);
    assert_eq!(
        worker_db.record_submit_receipt(
            &lease,
            now + Duration::minutes(1),
            "tok-sibling",
            now + Duration::minutes(10),
            SubmitChannel::Primary,
        )?,
        ReceiptWrite::Logged
    );
    assert_eq!(ctx.job(&sibling.id)?.token, None);
    Ok(())
}

#[test]
fn reschedule_clears_queued_cooldown_and_keeps_status_lease_and_stage() -> Result<()> {
    let ctx = Ctx::new()?;
    let now = t0();
    let job = ctx.create_backed_off_job(now)?;
    assert!(
        !ctx.ready_ids(now)?.contains(&job.id),
        "the cooldown must hold the job back"
    );

    // `None` keeps every field.
    ctx.db.reschedule(&job.id, None, None)?;
    assert_eq!(RowState::from(&ctx.job(&job.id)?), RowState::from(&job));

    ctx.db.reschedule(&job.id, Some(0), Some(None))?;
    let cleared = ctx.job(&job.id)?;
    assert_eq!(cleared.status, JobStatus::Queued);
    assert_eq!(cleared.attempt, 0);
    assert_eq!(cleared.next_poll_at, None);
    assert_eq!(cleared.last_error.as_deref(), Some(BACKOFF_ERROR));
    assert_eq!(cleared.submit_stage, None);
    assert_no_lease(&cleared);
    assert!(ctx.ready_ids(now)?.contains(&job.id));
    assert!(
        ctx.event_types(&job.id)?.is_empty(),
        "reschedule writes no event"
    );

    // A live claim survives a reset: lease and CLAIMED stage are kept, and the owner can
    // still dispatch.
    let mut lease = claim_submit(&ctx.other_handle(), &job.id, now)?;
    ctx.db
        .reschedule(&job.id, Some(2), Some(Some(now + Duration::minutes(5))))?;
    let claimed = ctx.job(&job.id)?;
    assert_eq!(claimed.status, JobStatus::Queued);
    assert_eq!(claimed.submit_stage, Some(SubmitStage::Claimed));
    assert_held_by(&claimed, &lease);
    assert_eq!(claimed.attempt, 2);
    assert_eq!(claimed.next_poll_at, Some(now + Duration::minutes(5)));
    assert!(ctx.other_handle().begin_submit_dispatch(
        &mut lease,
        SubmitChannel::Primary,
        now + Duration::minutes(1),
        SUBMIT_TTL,
    )?);

    assert!(
        ctx.db
            .reschedule("no-such-job", Some(0), Some(None))
            .is_err()
    );
    Ok(())
}

// ---------------------------------------------------------------------------------------
// 2. pull_poll_forward only moves a PROCESSING poll earlier
// ---------------------------------------------------------------------------------------

#[test]
fn pull_poll_forward_moves_processing_poll_earlier_never_later() -> Result<()> {
    let ctx = Ctx::new()?;
    let now = t0();
    let job = ctx.create_processing_job("tok-poll", now + Duration::minutes(10))?;
    let poll_lease = ctx
        .other_handle()
        .claim_job(&job.id, WorkKind::Poll, ClaimTiming::Now, now, POLL_TTL)?
        .context("poll claim must succeed")?;
    let leased = ctx.job(&job.id)?;
    assert_held_by(&leased, &poll_lease);

    let earlier = now + Duration::minutes(1);
    assert!(ctx.db.pull_poll_forward(&job.id, earlier)?);
    let pulled = ctx.job(&job.id)?;
    assert_eq!(
        RowState::from(&pulled),
        RowState {
            next_poll_at: Some(earlier),
            ..RowState::from(&leased)
        },
        "only next_poll_at may change; the poll lease is kept"
    );

    // Later, or equal: nothing changes.
    assert!(
        !ctx.db
            .pull_poll_forward(&job.id, now + Duration::minutes(5))?
    );
    assert!(!ctx.db.pull_poll_forward(&job.id, earlier)?);
    assert_eq!(RowState::from(&ctx.job(&job.id)?), RowState::from(&pulled));

    // The poll owner still holds the row.
    assert!(ctx.other_handle().release_lease(&poll_lease)?);
    assert!(ctx.event_types(&job.id)?.is_empty());
    Ok(())
}

#[test]
fn pull_poll_forward_ignores_jobs_that_are_not_processing() -> Result<()> {
    let ctx = Ctx::new()?;
    let now = t0();
    let at = now + Duration::minutes(1);

    let queued = ctx.create_backed_off_job(now)?;
    let dispatched = ctx.create_backed_off_job(now)?;
    dispatch(&ctx.other_handle(), &dispatched.id, now)?;
    let dispatched = ctx.job(&dispatched.id)?;
    let failed = ctx.create_backed_off_job(now)?;
    ctx.db.cancel_job(&failed.id, Some("wrong pdf"), now)?;
    let failed = ctx.job(&failed.id)?;
    assert_eq!(failed.status, JobStatus::Failed);

    for job in [&queued, &dispatched, &failed] {
        assert!(
            !ctx.db.pull_poll_forward(&job.id, at)?,
            "{} job must be left alone",
            job.status.as_str()
        );
        assert_eq!(RowState::from(&ctx.job(&job.id)?), RowState::from(job));
    }

    assert!(ctx.db.pull_poll_forward("no-such-job", at).is_err());
    Ok(())
}

/// `list_due_processing` and `claim_job(WhenDue)` treat a NULL `next_poll_at` as due now,
/// so "forward" to a future `at` would delay a poll that is already due.
/// `clear_sibling_job_cooldowns` leaves a PROCESSING sibling exactly like this.
#[test]
fn pull_poll_forward_does_not_delay_a_poll_that_is_already_due() -> Result<()> {
    let ctx = Ctx::new()?;
    let now = t0();
    let job = ctx.create_processing_job("tok-due", now + Duration::minutes(10))?;
    ctx.db.reschedule(&job.id, Some(0), Some(None))?;
    let due = ctx.job(&job.id)?;
    assert_eq!(due.next_poll_at, None);
    assert!(ctx.due_ids(now)?.contains(&job.id));

    assert!(
        !ctx.db
            .pull_poll_forward(&job.id, now + Duration::seconds(60))?,
        "a poll that is already due must not be pushed back"
    );
    assert_eq!(RowState::from(&ctx.job(&job.id)?), RowState::from(&due));
    assert!(ctx.due_ids(now)?.contains(&job.id));
    Ok(())
}

// ---------------------------------------------------------------------------------------
// 3. requeue refuses a submission in flight and otherwise resets the row
// ---------------------------------------------------------------------------------------

#[test]
fn requeue_refuses_dispatched_job_with_live_lease() -> Result<()> {
    let ctx = Ctx::new()?;
    let now = t0();
    let job = ctx.create_backed_off_job(now)?;
    let worker_db = ctx.other_handle();
    let lease = dispatch(&worker_db, &job.id, now)?;
    let before = ctx.job(&job.id)?;

    assert_eq!(
        ctx.db.requeue(&job.id, now + Duration::minutes(1))?,
        Requeue::InFlight {
            owner: lease.owner.clone(),
            expires_at: lease.expires_at,
        },
        "an in-flight submission must not be requeued"
    );
    assert_eq!(RowState::from(&ctx.job(&job.id)?), RowState::from(&before));
    assert_eq!(ctx.event_types(&job.id)?, ["submit_dispatched"]);

    assert_eq!(
        worker_db.record_submit_receipt(
            &lease,
            now + Duration::minutes(2),
            "tok-inflight",
            now + Duration::minutes(12),
            SubmitChannel::Primary,
        )?,
        ReceiptWrite::Accepted
    );
    let done = ctx.job(&job.id)?;
    assert_eq!(done.status, JobStatus::Processing);
    assert_eq!(done.token.as_deref(), Some("tok-inflight"));
    assert_eq!(done.submit_stage, None);
    assert_no_lease(&done);
    assert_eq!(
        ctx.event_types(&job.id)?,
        ["submit_dispatched", "submitted"]
    );
    Ok(())
}

#[test]
fn requeue_revokes_a_claim_that_has_not_dispatched() -> Result<()> {
    let ctx = Ctx::new()?;
    let now = t0();
    let job = ctx.create_backed_off_job(now)?;
    let worker_db = ctx.other_handle();
    let mut lease = claim_submit(&worker_db, &job.id, now)?;
    assert_eq!(ctx.job(&job.id)?.submit_stage, Some(SubmitStage::Claimed));

    ctx.db.requeue(&job.id, now + Duration::minutes(1))?;
    let requeued = assert_requeued(&ctx, &job.id, now + Duration::minutes(1))?;

    // The old owner can no longer send anything.
    assert!(!worker_db.begin_submit_dispatch(
        &mut lease,
        SubmitChannel::Primary,
        now + Duration::minutes(2),
        SUBMIT_TTL,
    )?);
    assert!(!worker_db.release_lease(&lease)?);
    assert_eq!(
        RowState::from(&ctx.job(&job.id)?),
        RowState::from(&requeued)
    );
    assert!(ctx.event_types(&job.id)?.is_empty());
    Ok(())
}

#[test]
fn requeue_resets_an_uncertain_submission() -> Result<()> {
    let ctx = Ctx::new()?;
    let now = t0();
    let job = ctx.create_backed_off_job(now)?;
    dispatch(&ctx.other_handle(), &job.id, now)?;
    let later = now + SUBMIT_TTL + Duration::minutes(1);
    assert_eq!(
        ctx.db.recover_expired_leases(PROJECT, later)?,
        LeaseRecovery {
            released_claims: 0,
            uncertain_submits: 1,
        }
    );
    let parked = ctx.job(&job.id)?;
    assert_eq!(parked.status, JobStatus::Submitted);
    assert_eq!(parked.submit_stage, Some(SubmitStage::Uncertain));
    assert_eq!(parked.attempt, 3);
    assert!(parked.last_error.is_some());

    ctx.db.requeue(&job.id, later)?;
    let requeued = assert_requeued(&ctx, &job.id, later)?;
    assert_eq!(requeued.token, None);
    assert_eq!(
        ctx.event_types(&job.id)?,
        ["submit_dispatched", "submit_outcome_unknown"]
    );
    Ok(())
}

#[test]
fn requeue_accepts_a_dispatch_whose_lease_has_expired() -> Result<()> {
    let ctx = Ctx::new()?;
    let now = t0();
    let job = ctx.create_backed_off_job(now)?;
    let worker_db = ctx.other_handle();
    let lease = dispatch(&worker_db, &job.id, now)?;

    // One second before expiry the lease is live; at expiry it is not.
    assert!(matches!(
        ctx.db
            .requeue(&job.id, lease.expires_at - Duration::seconds(1))?,
        Requeue::InFlight { .. }
    ));
    assert_eq!(
        ctx.db.requeue(&job.id, lease.expires_at)?,
        Requeue::Requeued
    );
    assert_requeued(&ctx, &job.id, lease.expires_at)?;

    // The owner's late receipt can no longer bind to a QUEUED job: only an event keeps it.
    assert_eq!(
        worker_db.record_submit_receipt(
            &lease,
            lease.expires_at + Duration::minutes(1),
            "tok-late",
            lease.expires_at + Duration::minutes(11),
            SubmitChannel::Primary,
        )?,
        ReceiptWrite::Logged
    );
    let after = assert_requeued(&ctx, &job.id, lease.expires_at)?;
    assert_eq!(after.token, None);
    assert_eq!(after.started_at, None);
    let events = ctx.events(&job.id)?;
    assert_eq!(
        ctx.event_types(&job.id)?,
        ["submit_dispatched", "submit_receipt_after_lease_lost"]
    );
    assert_eq!(events[1].payload["stored"], false);
    assert_eq!(events[1].payload["status"], "QUEUED");
    assert_eq!(events[1].payload["token"], "tok-late");
    Ok(())
}

#[test]
fn requeue_resets_a_failed_job() -> Result<()> {
    let ctx = Ctx::new()?;
    let now = t0();
    let job = ctx.create_backed_off_job(now)?;
    assert_eq!(
        ctx.db.cancel_job(&job.id, Some("wrong pdf"), now)?,
        CancelOutcome::Cancelled {
            previous_status: JobStatus::Queued,
            previous_stage: None,
            lease_was_active: false,
        }
    );
    let failed = ctx.job(&job.id)?;
    assert_eq!(failed.status, JobStatus::Failed);
    assert_eq!(failed.attempt, 3);
    assert_eq!(
        failed.last_error.as_deref(),
        Some("cancelled by user: wrong pdf")
    );

    ctx.db.requeue(&job.id, now)?;
    assert_requeued(&ctx, &job.id, now)?;
    assert!(ctx.db.requeue("no-such-job", now).is_err());
    Ok(())
}

// ---------------------------------------------------------------------------------------
// 4. A missing fallback script is definitive and leaves the fallback usable
// ---------------------------------------------------------------------------------------

/// The script is checked before `node` is spawned, so this needs no node install.
#[tokio::test]
async fn missing_fallback_script_fails_definitively_and_retry_reuses_fallback() -> Result<()> {
    let mut ctx = Ctx::new()?;
    let script = ctx.tmp.path().join("missing-fallback.cjs");
    assert!(!script.exists());
    ctx.use_fallback_script(&script);
    let job = ctx.create_queued_job()?;
    let backend = MockBackend::default()
        .on_submit(|| Answer::Now(Err(BackendError::Schema(REJECTION.to_string()))));
    let expected_error = format!(
        "primary submit error: schema error: {REJECTION}; fallback error: command error: fallback script not found: {}",
        script.display()
    );

    let assert_failed_needs_manual = |ctx: &Ctx| -> Result<()> {
        let failed = ctx.job(&job.id)?;
        assert_eq!(failed.status, JobStatus::FailedNeedsManual);
        assert_eq!(
            failed.submit_stage, None,
            "a missing script is not UNCERTAIN"
        );
        assert_eq!(failed.attempt, 1);
        assert_eq!(failed.next_poll_at, None);
        assert_eq!(failed.token, None);
        assert_eq!(failed.started_at, None);
        assert_eq!(failed.last_error.as_deref(), Some(expected_error.as_str()));
        assert!(
            !failed.fallback_used,
            "a fallback that never reached the provider must stay available"
        );
        assert_no_lease(&failed);
        Ok(())
    };

    assert_eq!(
        worker::submit_job_with_backend(&ctx.config, &ctx.db, &job.id, &backend).await?,
        Attempt::Ran
    );
    assert_eq!(backend.submit_count(), 1);
    assert_failed_needs_manual(&ctx)?;
    assert_eq!(
        ctx.event_types(&job.id)?,
        [
            "submit_dispatched",
            "submit_dispatched",
            "submit_failed_needs_manual"
        ]
    );
    assert_eq!(
        dispatch_channels(&ctx.events(&job.id)?),
        ["primary", "fallback"]
    );
    let events = ctx.events(&job.id)?;
    assert_eq!(events[2].payload, json!({ "reason": expected_error }));

    ctx.db.requeue(&job.id, Utc::now())?;
    let requeued = assert_requeued(&ctx, &job.id, Utc::now())?;
    assert!(!requeued.fallback_used);

    // The primary is rejected again, and the fallback is dispatched again.
    assert_eq!(
        worker::submit_job_with_backend(&ctx.config, &ctx.db, &job.id, &backend).await?,
        Attempt::Ran
    );
    assert_eq!(backend.submit_count(), 2);
    assert_failed_needs_manual(&ctx)?;
    assert_eq!(
        dispatch_channels(&ctx.events(&job.id)?),
        ["primary", "fallback", "primary", "fallback"]
    );
    assert_eq!(
        ctx.event_types(&job.id)?,
        [
            "submit_dispatched",
            "submit_dispatched",
            "submit_failed_needs_manual",
            "submit_dispatched",
            "submit_dispatched",
            "submit_failed_needs_manual"
        ]
    );
    Ok(())
}

/// `begin_submit_dispatch(Fallback)` sets `fallback_used` before the script runs; a
/// finish with `fallback_used: Some(false)` clears it, while `None` keeps it, so a
/// fallback with an unknown outcome is never rerun.
#[test]
fn fallback_flag_is_set_at_dispatch_and_follows_job_change() -> Result<()> {
    let ctx = Ctx::new()?;
    let now = t0();
    let later = now + Duration::minutes(1);

    let dispatch_fallback = |job: &Job| -> Result<Lease> {
        let mut lease = claim_submit(&ctx.db, &job.id, now)?;
        assert!(ctx.db.begin_submit_dispatch(
            &mut lease,
            SubmitChannel::Fallback,
            now,
            SUBMIT_TTL
        )?);
        assert!(lease.job.fallback_used);
        assert!(ctx.job(&job.id)?.fallback_used);
        assert_eq!(dispatch_channels(&ctx.events(&job.id)?), ["fallback"]);
        Ok(lease)
    };

    let never_reached = ctx.create_queued_job()?;
    let lease = dispatch_fallback(&never_reached)?;
    let change = JobChange {
        status: JobStatus::FailedNeedsManual,
        attempt: Some(1),
        next_poll_at: Some(None),
        last_error: Some(Some("fallback script not found".to_string())),
        submit_stage: None,
        fallback_used: Some(false),
    };
    assert_eq!(
        ctx.db.finish_lease(
            &lease,
            later,
            &change,
            "submit_failed_needs_manual",
            json!({})
        )?,
        LeaseWrite::Applied
    );
    let cleared = ctx.job(&never_reached.id)?;
    assert_eq!(cleared.status, JobStatus::FailedNeedsManual);
    assert!(!cleared.fallback_used);

    let unknown = ctx.create_queued_job()?;
    let lease = dispatch_fallback(&unknown)?;
    let change = JobChange {
        status: JobStatus::Submitted,
        attempt: Some(1),
        next_poll_at: Some(None),
        last_error: Some(Some("fallback outcome unknown".to_string())),
        submit_stage: Some(SubmitStage::Uncertain),
        fallback_used: None,
    };
    assert_eq!(
        ctx.db
            .finish_lease(&lease, later, &change, "submit_outcome_unknown", json!({}))?,
        LeaseWrite::Applied
    );
    let kept = ctx.job(&unknown.id)?;
    assert_eq!(kept.status, JobStatus::Submitted);
    assert_eq!(kept.submit_stage, Some(SubmitStage::Uncertain));
    assert!(kept.fallback_used);
    Ok(())
}

// ---------------------------------------------------------------------------------------
// 5. A stored receipt sets started_at
// ---------------------------------------------------------------------------------------

#[test]
fn stored_receipt_on_parked_submission_sets_started_at() -> Result<()> {
    let ctx = Ctx::new()?;
    let now = t0();
    let job = ctx.create_queued_job()?;
    let worker_db = ctx.other_handle();
    let lease = dispatch(&worker_db, &job.id, now)?;
    assert_eq!(ctx.job(&job.id)?.started_at, None);

    let late = lease.expires_at + Duration::minutes(1);
    assert_eq!(
        worker_db.record_submit_receipt(
            &lease,
            late,
            "tok-late",
            late + Duration::minutes(10),
            SubmitChannel::Primary,
        )?,
        ReceiptWrite::StoredForRecovery
    );
    let parked = ctx.job(&job.id)?;
    assert_eq!(parked.status, JobStatus::Submitted);
    assert_eq!(parked.submit_stage, Some(SubmitStage::Uncertain));
    assert_eq!(parked.token.as_deref(), Some("tok-late"));
    assert_eq!(
        parked.started_at,
        Some(late),
        "a stored token comes with started_at from the receipt time"
    );
    assert_no_lease(&parked);
    assert_eq!(parked.attempt, 0);
    assert_eq!(parked.next_poll_at, None);
    assert_eq!(
        parked.last_error.as_deref(),
        Some(
            format!(
                "submission receipt arrived after the worker lost its lease; a receipt token is saved; run `reviewloop retry --job-id {}` to resume polling",
                job.id
            )
            .as_str()
        )
    );
    let events = ctx.events(&job.id)?;
    assert_eq!(
        ctx.event_types(&job.id)?,
        ["submit_dispatched", "submit_receipt_after_lease_lost"]
    );
    assert_eq!(events[1].payload["stored"], true);
    assert_eq!(events[1].payload["status"], "SUBMITTED");
    assert_eq!(events[1].payload["token"], "tok-late");
    assert_eq!(events[1].payload["existing_token"], Value::Null);
    Ok(())
}

#[test]
fn stored_receipt_on_cancelled_job_sets_started_at() -> Result<()> {
    let ctx = Ctx::new()?;
    let now = t0();
    let job = ctx.create_queued_job()?;
    let worker_db = ctx.other_handle();
    let lease = dispatch(&worker_db, &job.id, now)?;
    assert_eq!(
        ctx.db
            .cancel_job(&job.id, Some("wrong pdf"), now + Duration::minutes(1))?,
        CancelOutcome::Cancelled {
            previous_status: JobStatus::Submitted,
            previous_stage: Some(SubmitStage::Dispatched),
            lease_was_active: true,
        }
    );

    let receipt_at = now + Duration::minutes(2);
    assert_eq!(
        worker_db.record_submit_receipt(
            &lease,
            receipt_at,
            "tok-cancelled",
            receipt_at + Duration::minutes(10),
            SubmitChannel::Primary,
        )?,
        ReceiptWrite::StoredForRecovery
    );
    let failed = ctx.job(&job.id)?;
    assert_eq!(failed.status, JobStatus::Failed);
    assert_eq!(failed.submit_stage, None);
    assert_eq!(failed.token.as_deref(), Some("tok-cancelled"));
    assert_eq!(failed.started_at, Some(receipt_at));
    assert_eq!(
        failed.last_error.as_deref(),
        Some("cancelled by user: wrong pdf")
    );
    assert_no_lease(&failed);
    let events = ctx.events(&job.id)?;
    assert_eq!(
        ctx.event_types(&job.id)?,
        [
            "submit_dispatched",
            "cancelled",
            "submit_receipt_after_lease_lost"
        ]
    );
    assert_eq!(events[2].payload["stored"], true);
    assert_eq!(events[2].payload["status"], "FAILED");
    Ok(())
}

/// The accepted path is unchanged by the fix: the first receipt sets `started_at` from
/// the wall clock at write time, and a later one keeps it.
#[test]
fn accepted_receipt_keeps_original_started_at_semantics() -> Result<()> {
    let ctx = Ctx::new()?;
    let now = t0();
    let job = ctx.create_queued_job()?;
    let lease = dispatch(&ctx.db, &job.id, now)?;

    let wall_before = Utc::now();
    assert_eq!(
        ctx.db.record_submit_receipt(
            &lease,
            now + Duration::minutes(1),
            "tok-first",
            now + Duration::minutes(11),
            SubmitChannel::Primary,
        )?,
        ReceiptWrite::Accepted
    );
    let wall_after = Utc::now();
    let accepted = ctx.job(&job.id)?;
    assert_eq!(accepted.status, JobStatus::Processing);
    assert_eq!(accepted.token.as_deref(), Some("tok-first"));
    let started_at = accepted
        .started_at
        .context("an accepted receipt must set started_at")?;
    assert!(
        wall_before <= started_at && started_at <= wall_after,
        "started_at {started_at} must come from the wall clock [{wall_before}, {wall_after}]"
    );

    // Back to QUEUED with the token and started_at kept, then accepted again.
    ctx.db
        .update_job_state(&job.id, JobStatus::Queued, Some(0), Some(None), Some(None))?;
    let requeued = ctx.job(&job.id)?;
    assert_eq!(requeued.started_at, Some(started_at));
    let later = now + Duration::hours(1);
    let lease = dispatch(&ctx.db, &job.id, later)?;
    assert_eq!(
        ctx.db.record_submit_receipt(
            &lease,
            later + Duration::minutes(1),
            "tok-second",
            later + Duration::minutes(11),
            SubmitChannel::Primary,
        )?,
        ReceiptWrite::Accepted
    );
    let again = ctx.job(&job.id)?;
    assert_eq!(again.status, JobStatus::Processing);
    assert_eq!(again.token.as_deref(), Some("tok-second"));
    assert_eq!(again.started_at, Some(started_at));
    Ok(())
}
