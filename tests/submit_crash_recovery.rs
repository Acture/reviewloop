//! OSS-337 acceptance (4) and (5) against a file-backed SQLite database and a mock
//! backend whose blocking and response loss the test controls:
//! - a worker that crashes before dispatch leaves a claim that is recovered and the job
//!   is then submitted exactly once;
//! - a submission whose outcome is unknown (crash after dispatch, lost response) is
//!   parked as SUBMITTED/UNCERTAIN and never resent or handed to the fallback.

mod common;

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use common::{
    Answer, Ctx, MockBackend, PAPER, PROJECT, SUBMIT_TTL, assert_no_lease, diagnostic,
    node_available, receipt,
};
use reviewloop::{
    backend::BackendError,
    db::{ClaimTiming, Db, JobChange, LeaseRecovery, LeaseWrite, ReceiptWrite},
    model::{EventRecord, Job, JobStatus, SubmitChannel, SubmitStage, WorkKind},
    worker::{self, Attempt},
};
use serde_json::Value;

impl Ctx {
    /// Every event of `event_type` on the paper, whichever job it belongs to.
    fn paper_events(&self, event_type: &str) -> Result<Vec<EventRecord>> {
        Ok(self
            .db
            .list_timeline_events(PROJECT, PAPER)?
            .into_iter()
            .filter(|event| event.event_type == event_type)
            .collect())
    }
}

fn only(events: Vec<EventRecord>, event_type: &str) -> EventRecord {
    assert_eq!(
        events.len(),
        1,
        "expected exactly one {event_type} event, got {events:?}"
    );
    events.into_iter().next().expect("length checked above")
}

fn claim_submit(db: &Db, job_id: &str, timing: ClaimTiming, now: DateTime<Utc>) -> Result<bool> {
    Ok(db
        .claim_job(job_id, WorkKind::Submit, timing, now, SUBMIT_TTL)?
        .is_some())
}

/// The job is parked SUBMITTED/UNCERTAIN: no lease, no claim path, no listing.
fn assert_parked_uncertain(db: &Db, job: &Job, now: DateTime<Utc>) -> Result<()> {
    assert_eq!(job.status, JobStatus::Submitted);
    assert_eq!(job.submit_stage, Some(SubmitStage::Uncertain));
    assert_no_lease(job);
    assert!(
        !claim_submit(db, &job.id, ClaimTiming::Now, now)?,
        "an uncertain submission must not be claimable for submit"
    );
    assert!(
        !claim_submit(db, &job.id, ClaimTiming::WhenDue, now)?,
        "an uncertain submission must not be claimable by the daemon"
    );
    assert!(
        db.list_ready_queued(PROJECT, 10, now)?.is_empty(),
        "an uncertain submission must not be listed as ready"
    );
    Ok(())
}

#[tokio::test]
async fn crash_before_dispatch_releases_expired_claim_then_submits_once() -> Result<()> {
    let ctx = Ctx::new()?;
    let job = ctx.create_queued_job()?;
    let now = Utc::now();

    let lease = ctx
        .db
        .claim_job(
            &job.id,
            WorkKind::Submit,
            ClaimTiming::WhenDue,
            now,
            SUBMIT_TTL,
        )?
        .context("a fresh QUEUED job must be claimable")?;
    let owner = lease.owner.clone();
    assert_eq!(lease.job.status, JobStatus::Queued);
    assert_eq!(lease.job.submit_stage, Some(SubmitStage::Claimed));
    assert_eq!(lease.job.lease_owner.as_deref(), Some(owner.as_str()));
    assert_eq!(lease.expires_at, now + SUBMIT_TTL);
    // Crash: the worker dies holding the claim; nothing was sent.
    drop(lease);

    let other = ctx.other_handle();
    assert!(
        !claim_submit(&other, &job.id, ClaimTiming::Now, now)?,
        "a live claim must exclude other workers"
    );
    assert!(other.list_ready_queued(PROJECT, 10, now)?.is_empty());
    assert_eq!(
        other.recover_expired_leases(PROJECT, now)?,
        LeaseRecovery::default(),
        "a live claim is not recovered"
    );
    let held = ctx.job(&job.id)?;
    assert_eq!(held.status, JobStatus::Queued);
    assert_eq!(held.submit_stage, Some(SubmitStage::Claimed));
    assert_eq!(held.lease_owner.as_deref(), Some(owner.as_str()));

    let later = now + Duration::minutes(31);
    assert_eq!(
        other.recover_expired_leases(PROJECT, later)?,
        LeaseRecovery {
            released_claims: 1,
            uncertain_submits: 0
        }
    );
    let released = ctx.job(&job.id)?;
    assert_eq!(released.status, JobStatus::Queued);
    assert_eq!(released.submit_stage, None);
    assert_no_lease(&released);
    assert_eq!(released.attempt, 0, "a released claim is not an attempt");
    assert_eq!(
        released.next_poll_at, job.next_poll_at,
        "cooldown unchanged"
    );
    assert_eq!(released.last_error, None);
    let expired = only(
        ctx.paper_events("submit_claim_expired")?,
        "submit_claim_expired",
    );
    assert_eq!(expired.job_id.as_deref(), Some(job.id.as_str()));
    assert_eq!(expired.payload["previous_owner"], owner.as_str());
    assert!(
        ctx.paper_events("submit_dispatched")?.is_empty(),
        "nothing was dispatched before the crash"
    );

    // A fresh worker in another process submits it, exactly once.
    let backend =
        MockBackend::default().on_submit(|| Answer::Now(Ok(receipt("tok-after-claim-crash"))));
    let attempt = worker::submit_job_with_backend(&ctx.config, &other, &job.id, &backend).await?;
    assert_eq!(attempt, Attempt::Ran);
    assert_eq!(backend.submit_count(), 1);

    let done = ctx.job(&job.id)?;
    assert_eq!(done.status, JobStatus::Processing);
    assert_eq!(done.token.as_deref(), Some("tok-after-claim-crash"));
    assert_eq!(done.submit_stage, None);
    assert_no_lease(&done);
    assert_eq!(done.last_error, None);
    assert!(done.next_poll_at.is_some(), "polling must be scheduled");
    let dispatched = only(ctx.paper_events("submit_dispatched")?, "submit_dispatched");
    assert_eq!(dispatched.payload["channel"], "primary");
    assert_ne!(dispatched.payload["owner"], owner.as_str());
    let submitted = only(ctx.paper_events("submitted")?, "submitted");
    assert_eq!(submitted.payload["token"], "tok-after-claim-crash");
    assert!(ctx.paper_events("submit_claim_taken_over")?.is_empty());

    // Nothing left to recover.
    assert_eq!(
        other.recover_expired_leases(PROJECT, later + Duration::hours(1))?,
        LeaseRecovery::default()
    );
    Ok(())
}

#[tokio::test]
async fn stalled_pre_dispatch_owner_cannot_send_after_takeover() -> Result<()> {
    let ctx = Ctx::new()?;
    let job = ctx.create_queued_job()?;
    let now = Utc::now();

    // The first worker claims, then stalls past its lease without sending anything.
    let mut stalled = ctx
        .db
        .claim_job(&job.id, WorkKind::Submit, ClaimTiming::Now, now, SUBMIT_TTL)?
        .context("a fresh QUEUED job must be claimable")?;

    // Another process takes the expired claim directly, without a recovery pass.
    let later = now + Duration::minutes(31);
    let other = ctx.other_handle();
    let taker = other
        .claim_job(
            &job.id,
            WorkKind::Submit,
            ClaimTiming::Now,
            later,
            SUBMIT_TTL,
        )?
        .context("an expired pre-dispatch claim must be claimable")?;
    assert_ne!(taker.owner, stalled.owner);
    assert_eq!(taker.job.submit_stage, Some(SubmitStage::Claimed));
    let takeover = only(
        ctx.paper_events("submit_claim_taken_over")?,
        "submit_claim_taken_over",
    );
    assert_eq!(takeover.payload["previous_owner"], stalled.owner.as_str());
    assert_eq!(takeover.payload["owner"], taker.owner.as_str());

    // The stalled worker wakes up: it must not dispatch nor release the new claim.
    let resume_at = later + Duration::minutes(1);
    assert!(
        !ctx.db.begin_submit_dispatch(
            &mut stalled,
            SubmitChannel::Primary,
            resume_at,
            SUBMIT_TTL
        )?,
        "a worker that lost its claim must not send"
    );
    assert!(!ctx.db.release_lease(&stalled)?);

    let row = ctx.job(&job.id)?;
    assert_eq!(row.status, JobStatus::Queued);
    assert_eq!(row.submit_stage, Some(SubmitStage::Claimed));
    assert_eq!(row.lease_owner.as_deref(), Some(taker.owner.as_str()));
    assert_eq!(row.lease_expires_at, Some(taker.expires_at));
    assert!(ctx.paper_events("submit_dispatched")?.is_empty());
    Ok(())
}

#[tokio::test]
async fn crash_after_dispatch_parks_uncertain_and_is_never_resubmitted() -> Result<()> {
    let ctx = Ctx::new()?;
    let job = ctx.create_queued_job()?;
    let backend =
        MockBackend::default().on_submit(|| Answer::OnRelease(Ok(receipt("tok-never-delivered"))));

    tokio::select! {
        result = worker::submit_job_with_backend(&ctx.config, &ctx.db, &job.id, &backend) => {
            panic!("a submit blocked in the provider must not finish: {result:?}");
        }
        () = backend.entered.notified() => {}
    }
    // The worker future was dropped mid-request: the process crashed after sending.
    assert_eq!(backend.submit_count(), 1);

    let now = Utc::now();
    let in_flight = ctx.job(&job.id)?;
    assert_eq!(in_flight.status, JobStatus::Submitted);
    assert_eq!(in_flight.submit_stage, Some(SubmitStage::Dispatched));
    let owner = in_flight
        .lease_owner
        .clone()
        .context("a dispatched job must carry its owner")?;
    let expires_at = in_flight
        .lease_expires_at
        .context("a dispatched job must carry its lease expiry")?;
    assert!(
        expires_at > now + Duration::minutes(29),
        "dispatch must renew the 30 min submit lease, got {expires_at}"
    );
    let dispatched = only(ctx.paper_events("submit_dispatched")?, "submit_dispatched");
    assert_eq!(dispatched.payload["channel"], "primary");
    assert_eq!(dispatched.payload["owner"], owner.as_str());

    // While the lease is live nobody may conclude the owner is gone.
    let other = ctx.other_handle();
    assert_eq!(
        other.recover_expired_leases(PROJECT, now)?,
        LeaseRecovery::default()
    );
    let untouched = ctx.job(&job.id)?;
    assert_eq!(untouched.status, JobStatus::Submitted);
    assert_eq!(untouched.submit_stage, Some(SubmitStage::Dispatched));
    assert_eq!(untouched.lease_owner.as_deref(), Some(owner.as_str()));
    assert!(ctx.paper_events("submit_outcome_unknown")?.is_empty());

    let later = now + Duration::minutes(31);
    assert_eq!(
        other.recover_expired_leases(PROJECT, later)?,
        LeaseRecovery {
            released_claims: 0,
            uncertain_submits: 1
        }
    );
    let parked = ctx.job(&job.id)?;
    assert_parked_uncertain(&other, &parked, later)?;
    assert_eq!(parked.token, None);
    let diagnostic = diagnostic(&parked)?;
    for needle in [
        "outcome unknown",
        "lost its lease",
        owner.as_str(),
        "import-token",
        "--force",
        "cancel",
    ] {
        assert!(
            diagnostic.contains(needle),
            "last_error must mention {needle:?}: {diagnostic}"
        );
    }
    assert!(diagnostic.contains(&parked.reconcile_hint()));
    let unknown = only(
        ctx.paper_events("submit_outcome_unknown")?,
        "submit_outcome_unknown",
    );
    assert_eq!(unknown.payload["source"], "lease_expired");
    assert_eq!(unknown.payload["previous_owner"], owner.as_str());
    assert_eq!(unknown.payload["reason"], diagnostic);

    // No path sends it again: by-id CLI submit, a full daemon tick, a stale-lease sweep.
    assert_eq!(
        worker::submit_job_with_backend(&ctx.config, &other, &job.id, &backend).await?,
        Attempt::NotClaimed
    );
    worker::run_tick(&ctx.config, &other).await?;
    worker::recover_stale_leases(&ctx.config, &other)?;
    assert_eq!(backend.submit_count(), 1);
    let after_tick = ctx.job(&job.id)?;
    assert_parked_uncertain(&other, &after_tick, later)?;
    assert_eq!(after_tick.last_error, parked.last_error);
    assert_eq!(after_tick.attempt, parked.attempt);

    // A second recovery pass is a no-op.
    assert_eq!(
        other.recover_expired_leases(PROJECT, later + Duration::hours(1))?,
        LeaseRecovery::default()
    );
    assert_eq!(ctx.paper_events("submit_outcome_unknown")?.len(), 1);
    assert_eq!(ctx.paper_events("submit_dispatched")?.len(), 1);

    // Reconciliation: both token intake paths still find the parked job.
    let email_target = other
        .find_latest_open_job_without_token("stanford")?
        .context("email ingestion must find the uncertain job")?;
    assert_eq!(email_target.id, job.id);
    let import_target = other
        .find_latest_open_job_for_paper(PROJECT, PAPER)?
        .context("import-token must find the uncertain job")?;
    assert_eq!(import_target.id, job.id);
    other.attach_token_to_job(&job.id, "tok-from-email", later)?;
    let reconciled = ctx.job(&job.id)?;
    assert_eq!(reconciled.status, JobStatus::Processing);
    assert_eq!(reconciled.token.as_deref(), Some("tok-from-email"));
    assert_eq!(reconciled.submit_stage, None);
    assert_no_lease(&reconciled);
    assert_eq!(reconciled.last_error, None);
    assert_eq!(reconciled.next_poll_at, Some(later));
    Ok(())
}

#[tokio::test]
async fn lost_response_parks_uncertain_without_fallback_or_resend() -> Result<()> {
    let mut ctx = Ctx::new()?;
    // Fallback fully enabled, so only the outcome classification keeps it from running.
    let marker = ctx.arm_marker_fallback()?;
    let job = ctx.create_queued_job()?;
    let backend = MockBackend::default().on_submit(|| {
        Answer::Now(Err(BackendError::OutcomeUnknown(
            "confirm-upload response lost".to_string(),
        )))
    });

    let attempt = worker::submit_job_with_backend(&ctx.config, &ctx.db, &job.id, &backend).await?;
    assert_eq!(attempt, Attempt::Ran);
    assert_eq!(backend.submit_count(), 1);

    let now = Utc::now();
    let parked = ctx.job(&job.id)?;
    assert_parked_uncertain(&ctx.db, &parked, now)?;
    assert_eq!(parked.attempt, job.attempt + 1);
    assert!(!parked.fallback_used, "fallback must not be dispatched");
    assert_eq!(parked.token, None);
    assert_eq!(parked.next_poll_at, None);
    let diagnostic = diagnostic(&parked)?;
    assert!(
        diagnostic.contains("outcome unknown (primary channel)"),
        "{diagnostic}"
    );
    assert!(
        diagnostic.contains("confirm-upload response lost"),
        "{diagnostic}"
    );
    assert!(
        diagnostic.contains(&parked.reconcile_hint()),
        "{diagnostic}"
    );
    // Only meaningful with node installed; the control test proves the marker works.
    assert!(!marker.exists(), "the fallback script must not run");

    let dispatched = only(ctx.paper_events("submit_dispatched")?, "submit_dispatched");
    assert_eq!(dispatched.payload["channel"], "primary");
    let unknown = only(
        ctx.paper_events("submit_outcome_unknown")?,
        "submit_outcome_unknown",
    );
    assert_eq!(unknown.payload["source"], "dispatch_error");
    assert_eq!(unknown.payload["channel"], "primary");
    assert_eq!(unknown.payload["error"], "confirm-upload response lost");
    for absent in [
        "submitted",
        "submitted_via_fallback",
        "submit_failed",
        "submit_failed_needs_manual",
    ] {
        assert!(
            ctx.paper_events(absent)?.is_empty(),
            "unexpected {absent} event"
        );
    }

    // Later attempts and sweeps never resend it.
    assert_eq!(
        worker::submit_job_with_backend(&ctx.config, &ctx.db, &job.id, &backend).await?,
        Attempt::NotClaimed
    );
    assert_eq!(
        ctx.other_handle()
            .recover_expired_leases(PROJECT, now + Duration::hours(1))?,
        LeaseRecovery::default()
    );
    assert_eq!(backend.submit_count(), 1);
    assert!(!marker.exists());
    let still = ctx.job(&job.id)?;
    assert_eq!(still.status, JobStatus::Submitted);
    assert_eq!(still.submit_stage, Some(SubmitStage::Uncertain));
    assert_eq!(still.attempt, parked.attempt);
    assert_eq!(ctx.paper_events("submit_dispatched")?.len(), 1);
    Ok(())
}

/// Control for the test above: with a definitive primary rejection the same fallback
/// configuration does run, so an absent marker there means the fallback was skipped.
#[tokio::test]
async fn definitive_primary_rejection_still_uses_fallback() -> Result<()> {
    if !node_available() {
        return Ok(());
    }
    let mut ctx = Ctx::new()?;
    let marker = ctx.arm_marker_fallback()?;
    let job = ctx.create_queued_job()?;
    let backend = MockBackend::default()
        .on_submit(|| Answer::Now(Err(BackendError::Schema("upload rejected".to_string()))));

    let attempt = worker::submit_job_with_backend(&ctx.config, &ctx.db, &job.id, &backend).await?;
    assert_eq!(attempt, Attempt::Ran);
    assert_eq!(backend.submit_count(), 1);
    assert!(marker.exists(), "the fallback script must run");

    let done = ctx.job(&job.id)?;
    assert_eq!(done.status, JobStatus::Processing);
    assert_eq!(done.token.as_deref(), Some("tok-fallback"));
    assert!(done.fallback_used);
    assert_eq!(done.submit_stage, None);
    assert_no_lease(&done);
    let channels: Vec<Value> = ctx
        .paper_events("submit_dispatched")?
        .into_iter()
        .map(|event| event.payload["channel"].clone())
        .collect();
    assert_eq!(channels, vec!["primary", "fallback"]);
    only(
        ctx.paper_events("submitted_via_fallback")?,
        "submitted_via_fallback",
    );
    Ok(())
}

#[tokio::test]
async fn in_flight_submit_excludes_competing_workers_then_completes_once() -> Result<()> {
    let ctx = Ctx::new()?;
    let job = ctx.create_queued_job()?;
    let backend =
        MockBackend::default().on_submit(|| Answer::OnRelease(Ok(receipt("tok-after-block"))));
    let other = ctx.other_handle();

    let competitors = async {
        backend.entered.notified().await;
        let observed = observe_in_flight(&ctx, &other, &job.id, &backend).await;
        // Release unconditionally so a failed check cannot hang the blocked worker.
        backend.release.notify_one();
        observed
    };
    let (attempt, observed) = tokio::join!(
        worker::submit_job_with_backend(&ctx.config, &ctx.db, &job.id, &backend),
        competitors,
    );
    observed?;
    assert_eq!(attempt?, Attempt::Ran);
    assert_eq!(backend.submit_count(), 1);

    let done = ctx.job(&job.id)?;
    assert_eq!(done.status, JobStatus::Processing);
    assert_eq!(done.token.as_deref(), Some("tok-after-block"));
    assert_eq!(done.submit_stage, None);
    assert_no_lease(&done);
    assert_eq!(done.last_error, None);
    only(ctx.paper_events("submit_dispatched")?, "submit_dispatched");
    only(ctx.paper_events("submitted")?, "submitted");
    assert!(ctx.paper_events("stale_result_rejected")?.is_empty());
    assert!(ctx.paper_events("submit_outcome_unknown")?.is_empty());
    Ok(())
}

/// Everything another process can try while the owner is blocked inside the provider.
async fn observe_in_flight(
    ctx: &Ctx,
    other: &Db,
    job_id: &str,
    backend: &MockBackend,
) -> Result<()> {
    let now = Utc::now();
    let row = ctx.job(job_id)?;
    assert_eq!(row.status, JobStatus::Submitted);
    assert_eq!(row.submit_stage, Some(SubmitStage::Dispatched));
    assert!(row.lease_owner.is_some());
    assert!(row.lease_expires_at.is_some_and(|at| at > now));

    assert!(!claim_submit(other, job_id, ClaimTiming::Now, now)?);
    assert!(!claim_submit(other, job_id, ClaimTiming::WhenDue, now)?);
    assert!(other.list_ready_queued(PROJECT, 10, now)?.is_empty());
    assert_eq!(
        other.recover_expired_leases(PROJECT, now)?,
        LeaseRecovery::default()
    );
    assert_eq!(
        worker::submit_job_with_backend(&ctx.config, other, job_id, backend).await?,
        Attempt::NotClaimed
    );

    // Competing claims from other threads, each on its own connection.
    let db_path = &ctx.db.path;
    let claimed = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..4)
            .map(|_| {
                scope.spawn(move || {
                    let db = Db::new_file(db_path.clone());
                    claim_submit(&db, job_id, ClaimTiming::Now, Utc::now())
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().expect("claim thread panicked"))
            .collect::<Result<Vec<bool>>>()
    })?;
    assert_eq!(claimed, vec![false; 4]);

    let still = ctx.job(job_id)?;
    assert_eq!(still.lease_owner, row.lease_owner);
    assert_eq!(still.submit_stage, Some(SubmitStage::Dispatched));
    assert_eq!(backend.submit_count(), 1);
    Ok(())
}

#[tokio::test]
async fn legacy_submitted_row_is_parked_without_blaming_a_lease() -> Result<()> {
    let ctx = Ctx::new()?;
    let job = ctx.create_queued_job()?;
    // An earlier version left the job SUBMITTED with no receipt, lease or stage.
    ctx.db
        .update_job_state_unchecked(&job.id, JobStatus::Submitted, None, None, None)?;
    let legacy = ctx.job(&job.id)?;
    assert_eq!(legacy.status, JobStatus::Submitted);
    assert_eq!(legacy.submit_stage, None);
    assert_no_lease(&legacy);

    let now = Utc::now();
    let other = ctx.other_handle();
    assert_eq!(
        other.recover_expired_leases(PROJECT, now)?,
        LeaseRecovery {
            released_claims: 0,
            uncertain_submits: 1
        }
    );
    let parked = ctx.job(&job.id)?;
    assert_parked_uncertain(&other, &parked, now)?;
    let diagnostic = diagnostic(&parked)?;
    assert!(diagnostic.contains("outcome unknown"), "{diagnostic}");
    assert!(
        diagnostic.contains("earlier reviewloop version"),
        "{diagnostic}"
    );
    assert!(
        diagnostic.contains(&parked.reconcile_hint()),
        "{diagnostic}"
    );
    assert!(
        !diagnostic.to_lowercase().contains("lease"),
        "a legacy row never had a lease: {diagnostic}"
    );
    let unknown = only(
        ctx.paper_events("submit_outcome_unknown")?,
        "submit_outcome_unknown",
    );
    assert_eq!(unknown.payload["source"], "legacy_submitted");
    assert_eq!(unknown.payload["previous_owner"], Value::Null);
    assert_eq!(unknown.payload["reason"], diagnostic);

    let backend = MockBackend::default().on_submit(|| Answer::Now(Ok(receipt("tok-unused"))));
    assert_eq!(
        worker::submit_job_with_backend(&ctx.config, &other, &job.id, &backend).await?,
        Attempt::NotClaimed
    );
    assert_eq!(backend.submit_count(), 0);
    assert_eq!(
        other.recover_expired_leases(PROJECT, now + Duration::hours(1))?,
        LeaseRecovery::default()
    );
    assert_eq!(ctx.paper_events("submit_outcome_unknown")?.len(), 1);
    Ok(())
}

#[tokio::test]
async fn receipt_after_crash_recovery_is_stored_but_does_not_resume() -> Result<()> {
    let ctx = Ctx::new()?;
    let job = ctx.create_queued_job()?;
    let now = Utc::now();

    // A worker dispatches, then stalls past its lease.
    let mut lease = ctx
        .db
        .claim_job(&job.id, WorkKind::Submit, ClaimTiming::Now, now, SUBMIT_TTL)?
        .context("a fresh QUEUED job must be claimable")?;
    assert!(
        ctx.db
            .begin_submit_dispatch(&mut lease, SubmitChannel::Primary, now, SUBMIT_TTL)?
    );
    let later = now + Duration::minutes(31);
    let other = ctx.other_handle();
    assert_eq!(
        other.recover_expired_leases(PROJECT, later)?,
        LeaseRecovery {
            released_claims: 0,
            uncertain_submits: 1
        }
    );

    // Its receipt finally arrives: kept for reconciliation, status untouched.
    let arrived = later + Duration::minutes(1);
    let write = ctx.db.record_submit_receipt(
        &lease,
        arrived,
        "tok-late",
        arrived + Duration::minutes(10),
        SubmitChannel::Primary,
    )?;
    assert_eq!(write, ReceiptWrite::StoredForRecovery);
    let parked = ctx.job(&job.id)?;
    assert_eq!(parked.status, JobStatus::Submitted);
    assert_eq!(parked.submit_stage, Some(SubmitStage::Uncertain));
    assert_eq!(parked.token.as_deref(), Some("tok-late"));
    assert_no_lease(&parked);
    let diagnostic = diagnostic(&parked)?;
    assert!(
        diagnostic.contains("receipt arrived after the worker lost its lease"),
        "{diagnostic}"
    );
    assert!(
        diagnostic.contains(&parked.reconcile_hint()),
        "{diagnostic}"
    );
    assert!(
        diagnostic.contains("receipt token is saved"),
        "{diagnostic}"
    );
    let late = only(
        ctx.paper_events("submit_receipt_after_lease_lost")?,
        "submit_receipt_after_lease_lost",
    );
    assert_eq!(late.payload["token"], "tok-late");
    assert_eq!(late.payload["stored"], true);
    assert_eq!(late.payload["status"], "SUBMITTED");
    assert_eq!(late.payload["owner"], lease.owner.as_str());
    assert!(ctx.paper_events("submitted")?.is_empty());

    // The stalled owner's own result is rejected, and the job stays out of the queue.
    let change = JobChange {
        status: JobStatus::Processing,
        attempt: Some(0),
        next_poll_at: Some(Some(arrived)),
        last_error: Some(None),
        submit_stage: None,
        fallback_used: None,
    };
    assert_eq!(
        ctx.db
            .finish_lease(&lease, arrived, &change, "submitted", serde_json::json!({}))?,
        LeaseWrite::Lost(Some(JobStatus::Submitted))
    );
    let still = ctx.job(&job.id)?;
    assert_parked_uncertain(&other, &still, arrived)?;
    assert_eq!(still.token.as_deref(), Some("tok-late"));
    assert_eq!(still.last_error, parked.last_error);
    Ok(())
}
