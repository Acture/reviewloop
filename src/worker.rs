use crate::{
    artifact::write_review_artifacts,
    backend::{
        BackendError, ReviewBackend, ReviewFetchResult, SubmitProgress, SubmitReceipt,
        SubmitRequest, build_backend,
        input::{InputVerdict, input_policy, upload_file_name},
        provider_source,
    },
    config::{Config, NotificationsConfig},
    db::{ClaimTiming, Db, JobChange, Lease, LeaseWrite, NewReview, ReceiptWrite},
    email::poll_imap_if_enabled,
    email_account::resolve_submission_email,
    fallback::submit_with_node_playwright,
    model::{Job, JobStatus, SubmitChannel, SubmitStage, WorkKind},
    notifier::{self, NotificationKind},
    panel::render_tick_panel,
    submission_input::{
        JobInput, SNAPSHOT_GC_GRACE, prune_unreferenced_snapshots, resolve_job_input,
    },
    trigger::{run_git_tag_trigger, run_pdf_trigger},
    util::compute_next_poll_at,
    widget_state,
};
use anyhow::{Context, Result};
use chrono::{Duration, Utc};
use serde_json::{Value, json};
use std::{
    fs,
    future::Future,
    io::Write,
    path::{Path, PathBuf},
    time::Duration as StdDuration,
};
use tracing::{error, info, warn};

/// Submit lease length, renewed at every dispatch. It outlives `SUBMIT_CALL_TIMEOUT`, so a
/// running owner records its own outcome first. The lease is wall-clock while the timeout
/// pauses during system sleep, so a suspended owner can still lose its lease mid-call:
/// its late result is then rejected but kept for recovery.
const SUBMIT_LEASE_TTL: Duration = Duration::minutes(30);
/// Upper bound on one dispatch (primary call or fallback script).
const SUBMIT_CALL_TIMEOUT: StdDuration = StdDuration::from_secs(20 * 60);
const POLL_LEASE_TTL: Duration = Duration::minutes(10);
const POLL_CALL_TIMEOUT: StdDuration = StdDuration::from_secs(5 * 60);

/// Offload a notification call onto a blocking thread so a slow or absent
/// OS notification daemon (NSUserNotificationCenter, D-Bus) cannot stall the
/// tokio runtime.  The JoinHandle is intentionally detached — if the call
/// fails, `notifier::notify` logs a `warn!` and returns; the daemon tick
/// continues unaffected.
///
/// Falls back to a direct (synchronous) call when invoked outside a tokio
/// runtime (e.g., in unit tests that call sync worker functions directly).
fn fire_notification(
    cfg: &NotificationsConfig,
    kind: NotificationKind,
    paper_id: Option<&str>,
    job_id: Option<&str>,
    body: Option<&str>,
) {
    let cfg = cfg.clone();
    let paper_id = paper_id.map(str::to_string);
    let job_id = job_id.map(str::to_string);
    let body = body.map(str::to_string);
    if tokio::runtime::Handle::try_current().is_ok() {
        tokio::task::spawn_blocking(move || {
            notifier::notify(
                &cfg,
                kind,
                paper_id.as_deref(),
                job_id.as_deref(),
                body.as_deref(),
            );
        });
    } else {
        // Sync context (e.g., unit tests without a Tokio runtime).
        notifier::notify(
            &cfg,
            kind,
            paper_id.as_deref(),
            job_id.as_deref(),
            body.as_deref(),
        );
    }
}

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
            // Persist the failure so `daemon status` can surface it without
            // tailing the daemon log. The next tick can read this back via
            // db.most_recent_event_of_type(_, "tick_failed").
            if let Err(persist_err) = db.add_event(
                Some(&config.project_id),
                None,
                "tick_failed",
                json!({ "tick": tick, "error": msg.clone() }),
            ) {
                // Don't let an event-write failure mask the underlying tick
                // failure or kill the daemon; log and continue.
                warn!(
                    tick,
                    error = %persist_err,
                    "failed to persist tick_failed event"
                );
            }
            fire_notification(
                &config.notifications,
                NotificationKind::TickError,
                None,
                None,
                Some(&msg),
            );
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
    // NOTE: span is entered with `.entered()`. Span context is carried across
    // the sync portions of this function but will not propagate through .await
    // boundaries in called async fns — each of those enters its own span.
    let _span = tracing::info_span!(
        "run_tick",
        tick = tick.unwrap_or(0),
        project_id = %config.project_id
    )
    .entered();

    run_git_tag_trigger(config, db)?;
    run_pdf_trigger(config, db)?;

    let email_polled_jobs = poll_imap_if_enabled(config, db).await?;

    mark_timeouts(config, db)?;
    recover_stale_leases(config, db)?;
    process_submissions(config, db).await?;
    process_polls(config, db).await?;

    // Immediately poll any jobs that just received a token via email ingestion,
    // rather than waiting for the next 30-second tick.
    for job in email_polled_jobs {
        if let Some(fresh) = db.get_job(&job.id)?
            && fresh.status == JobStatus::Processing
            && fresh.token.is_some()
        {
            poll_job(config, db, &fresh.id).await?;
        }
    }

    prune_retention(config, db, tick)?;

    if let Some(path) = config.widget_state_path() {
        if let Err(e) = widget_state::build_and_write(config, db, &path) {
            tracing::warn!(error = %e, "failed to write widget state file");
        }
    }

    Ok(())
}

/// Outcome of a by-id worker entry point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Attempt {
    /// This call held the job's lease and ran the work.
    Ran,
    /// The job was not claimable: another worker holds it, or its status changed.
    NotClaimed,
}

/// Settle submit attempts whose worker vanished; see [`Db::recover_expired_leases`].
pub fn recover_stale_leases(config: &Config, db: &Db) -> Result<()> {
    let report = db.recover_expired_leases(&config.project_id, Utc::now())?;
    if report.released_claims > 0 {
        info!(
            released = report.released_claims,
            "released expired submit claims that never dispatched"
        );
    }
    if report.uncertain_submits > 0 {
        warn!(
            uncertain = report.uncertain_submits,
            "submissions with unknown outcome need reconciliation"
        );
        fire_notification(
            &config.notifications,
            NotificationKind::FailedNeedsManual,
            None,
            None,
            Some(&format!(
                "{} submission(s) may have reached the provider without a saved receipt; see `reviewloop status`",
                report.uncertain_submits
            )),
        );
    }
    Ok(())
}

pub async fn process_submissions(config: &Config, db: &Db) -> Result<()> {
    let per_tick_budget = usize::min(
        config.core.max_concurrency,
        config.core.max_submissions_per_tick,
    );
    for job in db.list_ready_queued(&config.project_id, per_tick_budget, Utc::now())? {
        // Another process may have claimed or rescheduled it since the listing.
        let Some(lease) = db.claim_job(
            &job.id,
            WorkKind::Submit,
            ClaimTiming::WhenDue,
            Utc::now(),
            SUBMIT_LEASE_TTL,
        )?
        else {
            continue;
        };
        let backend = match build_backend(
            config,
            &lease.job.backend,
            Some(db),
            Some(&config.project_id),
        ) {
            Ok(backend) => backend,
            Err(err) => return Err(abandon_claim(db, &lease, err)),
        };
        submit_leased(config, db, lease, backend.as_ref()).await?;
    }

    Ok(())
}

pub async fn process_polls(config: &Config, db: &Db) -> Result<()> {
    let jobs =
        db.list_due_processing(&config.project_id, config.core.max_concurrency, Utc::now())?;
    for job in jobs {
        let Some(lease) = db.claim_job(
            &job.id,
            WorkKind::Poll,
            ClaimTiming::WhenDue,
            Utc::now(),
            POLL_LEASE_TTL,
        )?
        else {
            continue;
        };
        let backend = match build_backend(
            config,
            &lease.job.backend,
            Some(db),
            Some(&config.project_id),
        ) {
            Ok(backend) => backend,
            Err(err) => return Err(abandon_claim(db, &lease, err)),
        };
        poll_leased(config, db, lease, backend.as_ref()).await?;
    }
    Ok(())
}

/// Submit one QUEUED job now, ignoring its cooldown (explicit CLI action).
pub async fn submit_job(config: &Config, db: &Db, job_id: &str) -> Result<Attempt> {
    let job = project_job(config, db, job_id)?;
    let backend = build_backend(config, &job.backend, Some(db), Some(&config.project_id))?;
    submit_job_with_backend(config, db, job_id, backend.as_ref()).await
}

/// [`submit_job`] against a caller-supplied primary backend.
pub async fn submit_job_with_backend(
    config: &Config,
    db: &Db,
    job_id: &str,
    backend: &dyn ReviewBackend,
) -> Result<Attempt> {
    project_job(config, db, job_id)?;
    let Some(lease) = db.claim_job(
        job_id,
        WorkKind::Submit,
        ClaimTiming::Now,
        Utc::now(),
        SUBMIT_LEASE_TTL,
    )?
    else {
        info!(
            job_id,
            "submit skipped: job is not QUEUED or another worker holds it"
        );
        return Ok(Attempt::NotClaimed);
    };
    submit_leased(config, db, lease, backend).await?;
    Ok(Attempt::Ran)
}

/// Everything a submit attempt needs, resolved before anything is sent: after dispatch
/// a local error would strand the job mid-flight, where it reads as an unknown outcome.
struct SubmitPlan {
    request: SubmitRequest,
    fallback: Option<FallbackPlan>,
}

/// The same manuscript, email and venue as the primary request, sent by the fallback
/// script; it reports its own progress.
struct FallbackPlan {
    script: PathBuf,
    base_url: String,
    pdf_path: PathBuf,
    file_name: String,
    email: String,
    venue: Option<String>,
    progress: SubmitProgress,
}

impl SubmitPlan {
    /// `None` when the job's pinned PDF cannot be found; [`pinned_input`] has then
    /// moved it to FAILED_NEEDS_MANUAL, which also revokes the claim.
    fn prepare(config: &Config, db: &Db, job: &Job) -> Result<Option<Self>> {
        let Some(snapshot_path) = pinned_input(config, db, job)? else {
            return Ok(None);
        };
        let email = resolve_submission_email(config, &job.backend, Some(&job.email))?;
        // Prefer the venue stored on the job. The paper may have been removed from
        // config since enqueue; the snapshot is self-contained, so the config is
        // only a venue fallback.
        let venue = match job.backend.as_str() {
            "stanford" => job
                .venue
                .as_deref()
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .map(str::to_string)
                .or_else(|| {
                    config
                        .find_paper(&job.paper_id)
                        .and_then(|p| config.venue_for(p))
                }),
            _ => job.venue.clone(),
        };
        let fallback = if job.backend == "stanford"
            && !job.fallback_used
            && config.providers.stanford.fallback_mode == "node_playwright"
        {
            Some(FallbackPlan {
                script: PathBuf::from(&config.providers.stanford.fallback_script),
                base_url: config.providers.stanford.base_url.clone(),
                file_name: upload_file_name(&snapshot_path).with_context(|| {
                    format!(
                        "snapshot has no usable file name: {}",
                        snapshot_path.display()
                    )
                })?,
                pdf_path: snapshot_path.clone(),
                email: email.clone(),
                venue: venue.clone(),
                progress: SubmitProgress::default(),
            })
        } else {
            None
        };
        Ok(Some(Self {
            request: SubmitRequest {
                pdf_path: snapshot_path,
                email,
                venue,
                review_options: job.review_options.clone(),
                progress: SubmitProgress::default(),
            },
            fallback,
        }))
    }
}

/// Resolve the verified snapshot `job` must upload, backfilling one for jobs
/// enqueued before snapshots existed. Returns `None` after moving the job to
/// FAILED_NEEDS_MANUAL when no bytes matching `job.pdf_hash` can be found, so
/// a different PDF is never submitted under this job's identity.
fn pinned_input(config: &Config, db: &Db, job: &Job) -> Result<Option<PathBuf>> {
    match resolve_job_input(&config.state_dir(), job)? {
        JobInput::Ready(snapshot_path) => Ok(Some(snapshot_path)),
        JobInput::Backfilled(input) => {
            db.set_job_snapshot(&job.id, &input.snapshot_path)?;
            db.add_event(
                None,
                Some(&job.id),
                "snapshot_backfilled",
                json!({
                    "pdf_path": job.pdf_path,
                    "pdf_hash": job.pdf_hash,
                    "snapshot_path": input.snapshot_path,
                }),
            )?;
            info!(job_id = %job.id, snapshot = %input.snapshot_path.display(), "backfilled PDF snapshot for job");
            Ok(Some(input.snapshot_path))
        }
        JobInput::Blocked { reason } => {
            // Retry with --force so resubmission follows the restore at once:
            // a PDF watcher that sees the restored file first enqueues it as a
            // new job, which makes the retry unnecessary.
            let message = format!(
                "submission blocked: {reason}; expected sha256 {hash}. Recover by restoring that \
                 version at {source} and immediately running `reviewloop retry --job-id {id} --force` \
                 (skip the retry if the PDF watcher has already enqueued the restored file), or \
                 review the current file instead with `reviewloop submit --paper-id {paper}`",
                hash = job.pdf_hash,
                source = job.pdf_path,
                id = job.id,
                paper = job.paper_id,
            );
            db.update_job_state(
                &job.id,
                JobStatus::FailedNeedsManual,
                Some(job.attempt),
                Some(None),
                Some(Some(message.clone())),
            )?;
            db.add_event(
                None,
                Some(&job.id),
                "submit_blocked_input_mismatch",
                json!({
                    "reason": reason,
                    "pdf_path": job.pdf_path,
                    "pdf_hash": job.pdf_hash,
                    "snapshot_path": job.snapshot_path,
                }),
            )?;
            fire_notification(
                &config.notifications,
                NotificationKind::FailedNeedsManual,
                Some(&job.paper_id),
                Some(&job.id),
                Some(&message),
            );
            error!(job_id = %job.id, reason = %reason, "submission blocked: no PDF matches the job's pinned hash");
            Ok(None)
        }
    }
}

async fn submit_leased(
    config: &Config,
    db: &Db,
    mut lease: Lease,
    backend: &dyn ReviewBackend,
) -> Result<()> {
    // NOTE: span is entered here; context is carried through sync code but
    // not propagated across .await points (pragmatic trade-off over a full
    // async body rewrite — still provides structured context on function entry).
    let _span = tracing::info_span!(
        "submit_job",
        job_id = %lease.job.id,
        paper_id = %lease.job.paper_id,
        backend = %lease.job.backend,
        attempt = lease.job.attempt
    )
    .entered();

    let plan = match SubmitPlan::prepare(config, db, &lease.job) {
        Ok(Some(plan)) => plan,
        Ok(None) => return Ok(()),
        Err(err) => return Err(abandon_claim(db, &lease, err)),
    };
    match preflight(config, db, &lease, &plan.request.pdf_path) {
        Ok(true) => {}
        Ok(false) => return Ok(()),
        Err(err) => return Err(abandon_claim(db, &lease, err)),
    }

    if !db.begin_submit_dispatch(
        &mut lease,
        SubmitChannel::Primary,
        Utc::now(),
        SUBMIT_LEASE_TTL,
    )? {
        warn!(job_id = %lease.job.id, "submit lease lost before dispatch; nothing sent");
        return Ok(());
    }

    let progress = plan.request.progress.clone();
    let primary = Dispatch {
        channel: SubmitChannel::Primary,
        progress: &progress,
    };
    match bounded_submit(backend.submit(plan.request)).await {
        Ok(receipt) => accept_receipt(config, db, &lease, &receipt, SubmitChannel::Primary),
        Err(BackendError::OutcomeUnknown(detail)) => {
            mark_uncertain(config, db, &lease, primary, &detail)
        }
        Err(BackendError::RateLimited {
            message,
            retry_after,
        }) => schedule_submit_retry(config, db, &lease, primary, message, retry_after),
        // Nothing was created, but no other job of this backend can succeed until
        // someone fixes the credentials: say so instead of failing quietly.
        Err(err @ BackendError::Auth(_)) => {
            park_submit_needs_manual(config, db, &lease, primary, err)
        }
        // The provider provably rejected the request, so another route cannot duplicate it.
        Err(err) => match plan.fallback {
            Some(fallback) => {
                let primary_step = progress.current();
                submit_via_fallback(config, db, lease, fallback, err, primary_step).await
            }
            None => fail_submit(db, &lease, primary, err),
        },
    }
}

/// Which route a dispatch took and the step it reached, for its outcome event.
#[derive(Clone, Copy)]
struct Dispatch<'a> {
    channel: SubmitChannel,
    progress: &'a SubmitProgress,
}

impl Dispatch<'_> {
    /// `payload` with the step this dispatch reached, when the backend reported one.
    fn with_step(&self, mut payload: Value) -> Value {
        if let (Some(step), Some(fields)) = (self.progress.current(), payload.as_object_mut()) {
            fields.insert("step".to_string(), Value::from(step.as_str()));
        }
        payload
    }
}

/// Check the pinned PDF against the provider's published limits before anything is
/// sent. Returns `false` after moving the job to FAILED_NEEDS_MANUAL when the provider
/// would refuse it; a rejected input never reaches the provider or the fallback, whose
/// form would refuse it client-side and leave the outcome looking unknown.
fn preflight(config: &Config, db: &Db, lease: &Lease, pdf_path: &Path) -> Result<bool> {
    let job = &lease.job;
    let Some(policy) = input_policy(&job.backend) else {
        return Ok(true);
    };
    match policy.check(pdf_path)? {
        InputVerdict::Accepted {
            estimated_pages,
            notices,
        } => {
            if !notices.is_empty() {
                for notice in &notices {
                    warn!(job_id = %job.id, notice = %notice, "provider will not review the whole PDF");
                }
                db.add_event(
                    None,
                    Some(&job.id),
                    "submit_input_notice",
                    json!({
                        "estimated_pages": estimated_pages,
                        "reviewed_pages": policy.reviewed_pages,
                        "notices": notices,
                    }),
                )?;
            }
            Ok(true)
        }
        InputVerdict::Rejected { reason } => {
            let message = format!(
                "submission blocked: {reason}. Nothing was sent. Request a review of the \
                 fixed PDF with `reviewloop submit --paper-id {}`",
                job.paper_id
            );
            let change = JobChange {
                status: JobStatus::FailedNeedsManual,
                attempt: Some(job.attempt),
                next_poll_at: Some(None),
                last_error: Some(Some(message.clone())),
                submit_stage: None,
                fallback_used: None,
            };
            if finish(
                db,
                lease,
                &change,
                "submit_input_rejected",
                json!({
                    "reason": reason,
                    "pdf_hash": job.pdf_hash,
                    "snapshot_path": job.snapshot_path,
                }),
            )? {
                fire_notification(
                    &config.notifications,
                    NotificationKind::FailedNeedsManual,
                    Some(&job.paper_id),
                    Some(&job.id),
                    Some(&message),
                );
                error!(job_id = %job.id, reason = %reason, "submission blocked: the provider would refuse this PDF");
            }
            Ok(false)
        }
    }
}

async fn submit_via_fallback(
    config: &Config,
    db: &Db,
    mut lease: Lease,
    fallback: FallbackPlan,
    primary_err: BackendError,
    primary_step: Option<crate::backend::SubmitStep>,
) -> Result<()> {
    if !db.begin_submit_dispatch(
        &mut lease,
        SubmitChannel::Fallback,
        Utc::now(),
        SUBMIT_LEASE_TTL,
    )? {
        warn!(job_id = %lease.job.id, error = %primary_err, "submit lease lost after primary failure; fallback not started");
        let current = db.get_job(&lease.job.id)?.map(|job| job.status);
        applied(db, &lease, LeaseWrite::Lost(current), "submit_failed")?;
        return Ok(());
    }

    let result = bounded_submit(submit_with_node_playwright(
        &fallback.script,
        &fallback.base_url,
        &fallback.pdf_path,
        &fallback.file_name,
        &fallback.email,
        fallback.venue.as_deref(),
        &fallback.progress,
    ))
    .await;
    let dispatch = Dispatch {
        channel: SubmitChannel::Fallback,
        progress: &fallback.progress,
    };
    let primary_err = match primary_step {
        Some(step) => format!("{primary_err} (at {})", step.as_str()),
        None => primary_err.to_string(),
    };
    match result {
        Ok(receipt) => accept_receipt(config, db, &lease, &receipt, SubmitChannel::Fallback),
        Err(BackendError::OutcomeUnknown(detail)) => mark_uncertain(
            config,
            db,
            &lease,
            dispatch,
            &format!("primary submit error: {primary_err}; fallback: {detail}"),
        ),
        // Nothing was created, so a later attempt may take either route again.
        Err(BackendError::RateLimited {
            message,
            retry_after,
        }) => schedule_submit_retry(config, db, &lease, dispatch, message, retry_after),
        Err(fallback_err) => {
            let reason =
                format!("primary submit error: {primary_err}; fallback error: {fallback_err}");
            let change = JobChange {
                status: JobStatus::FailedNeedsManual,
                attempt: Some(lease.job.attempt + 1),
                next_poll_at: Some(None),
                last_error: Some(Some(reason.clone())),
                submit_stage: None,
                // The fallback provably never reached the provider; a later retry may use it.
                fallback_used: Some(false),
            };
            if finish(
                db,
                &lease,
                &change,
                "submit_failed_needs_manual",
                dispatch.with_step(json!({ "reason": reason, "channel": "fallback" })),
            )? {
                fire_notification(
                    &config.notifications,
                    NotificationKind::FailedNeedsManual,
                    Some(&lease.job.paper_id),
                    Some(&lease.job.id),
                    Some(&reason),
                );
                error!(job_id = %lease.job.id, "submit failed and fallback failed; manual intervention required");
            }
            Ok(())
        }
    }
}

/// Bound one dispatch. Giving up after the request may have been sent leaves its
/// outcome unknown.
async fn bounded_submit(
    call: impl Future<Output = Result<SubmitReceipt, BackendError>>,
) -> Result<SubmitReceipt, BackendError> {
    tokio::time::timeout(SUBMIT_CALL_TIMEOUT, call)
        .await
        .unwrap_or_else(|_| {
            Err(BackendError::OutcomeUnknown(format!(
                "no response within {}s",
                SUBMIT_CALL_TIMEOUT.as_secs()
            )))
        })
}

fn accept_receipt(
    config: &Config,
    db: &Db,
    lease: &Lease,
    receipt: &SubmitReceipt,
    channel: SubmitChannel,
) -> Result<()> {
    let next_poll = compute_next_poll_at(
        Utc::now(),
        &config.polling.schedule_minutes,
        0,
        config.polling.jitter_percent,
    );
    let job_id = &lease.job.id;
    let write = match db.record_submit_receipt(
        lease,
        Utc::now(),
        &receipt.token,
        next_poll,
        channel,
    ) {
        Ok(write) => write,
        Err(err) => {
            // The provider holds this submission: keep its token where the operator can
            // reach it without writing it to the daemon log or a notification.
            let context = match keep_unsaved_receipt(config, lease, receipt, channel) {
                Ok(kept) => format!(
                    "failed to save the submit receipt for job {job_id}; its token is kept in {} \
                     (attach it with `reviewloop import-token --job-id {job_id} --token <token>`)",
                    kept.display()
                ),
                // Last resort: with neither the database nor the state dir writable, this
                // log line is the only place left to keep the token. The error itself also
                // reaches notifications, `daemon status` and the widget, so it stays clean.
                Err(keep_err) => {
                    error!(
                        job_id = %job_id,
                        channel = channel.as_str(),
                        token = %receipt.token,
                        "submit receipt could neither be saved nor kept on disk; its token is in this line only"
                    );
                    format!(
                        "failed to save the submit receipt for job {job_id} and to keep it on disk \
                         ({keep_err:#}); its token is in the daemon log"
                    )
                }
            };
            return Err(err.context(context));
        }
    };
    match write {
        ReceiptWrite::Accepted => match channel {
            SubmitChannel::Primary => info!(job_id = %job_id, "job submitted"),
            SubmitChannel::Fallback => warn!(job_id = %job_id, "job submitted via fallback script"),
        },
        ReceiptWrite::StoredForRecovery => {
            warn!(
                job_id = %job_id,
                "submit receipt arrived after the lease was lost; token stored for recovery, status unchanged"
            );
            // A parked submission now has a usable token: tell the operator how to resume.
            if let Some(job) = db.get_job(job_id)?
                && job.status == JobStatus::Submitted
            {
                fire_notification(
                    &config.notifications,
                    NotificationKind::FailedNeedsManual,
                    Some(&job.paper_id),
                    Some(&job.id),
                    job.last_error.as_deref(),
                );
            }
        }
        ReceiptWrite::Logged => warn!(
            job_id = %job_id,
            "submit receipt arrived after the lease was lost; recorded as an event only"
        ),
    }
    Ok(())
}

/// Write a receipt the database refused to `<state_dir>/recovery/`, readable only by
/// its owner. Returns the file's path.
fn keep_unsaved_receipt(
    config: &Config,
    lease: &Lease,
    receipt: &SubmitReceipt,
    channel: SubmitChannel,
) -> Result<PathBuf> {
    let dir = config.state_dir().join("recovery");
    fs::create_dir_all(&dir).with_context(|| format!("failed to create {}", dir.display()))?;
    let path = dir.join(format!(
        "receipt-{}-{}.json",
        lease.job.id,
        Utc::now().format("%Y%m%dT%H%M%S%.3fZ")
    ));
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let mut file = options
        .open(&path)
        .with_context(|| format!("failed to create {}", path.display()))?;
    let record = json!({
        "job_id": lease.job.id,
        "paper_id": lease.job.paper_id,
        "backend": lease.job.backend,
        "channel": channel.as_str(),
        "token": receipt.token,
        "received_at": Utc::now().to_rfc3339(),
    });
    file.write_all(serde_json::to_string_pretty(&record)?.as_bytes())
        .and_then(|()| file.sync_all())
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(path)
}

/// The provider may hold the submission: park the job as SUBMITTED/UNCERTAIN. It is
/// never resubmitted or handed to the fallback automatically.
fn mark_uncertain(
    config: &Config,
    db: &Db,
    lease: &Lease,
    dispatch: Dispatch<'_>,
    detail: &str,
) -> Result<()> {
    let channel = dispatch.channel;
    let reason = format!(
        "submission outcome unknown ({} channel): {detail}; {}",
        channel.as_str(),
        lease.job.reconcile_hint()
    );
    let change = JobChange {
        status: JobStatus::Submitted,
        attempt: Some(lease.job.attempt + 1),
        next_poll_at: Some(None),
        last_error: Some(Some(reason.clone())),
        submit_stage: Some(SubmitStage::Uncertain),
        fallback_used: None,
    };
    if finish(
        db,
        lease,
        &change,
        "submit_outcome_unknown",
        dispatch.with_step(
            json!({ "source": "dispatch_error", "channel": channel.as_str(), "error": detail }),
        ),
    )? {
        fire_notification(
            &config.notifications,
            NotificationKind::FailedNeedsManual,
            Some(&lease.job.paper_id),
            Some(&lease.job.id),
            Some(&reason),
        );
        error!(job_id = %lease.job.id, channel = channel.as_str(), "submission outcome unknown; awaiting reconciliation");
    }
    Ok(())
}

fn schedule_submit_retry(
    config: &Config,
    db: &Db,
    lease: &Lease,
    dispatch: Dispatch<'_>,
    message: String,
    retry_after: Option<Duration>,
) -> Result<()> {
    let attempt = lease.job.attempt + 1;
    let next = match retry_after {
        Some(d) => Utc::now() + d,
        None => compute_next_poll_at(
            Utc::now(),
            &config.polling.schedule_minutes,
            attempt,
            config.polling.jitter_percent,
        ),
    };
    let retry_after_source = if retry_after.is_some() {
        "server"
    } else {
        "schedule"
    };
    let change = JobChange {
        status: JobStatus::Queued,
        attempt: Some(attempt),
        next_poll_at: Some(Some(next)),
        last_error: Some(Some(message.clone())),
        submit_stage: None,
        // A rate-limited fallback created nothing, so it stays available.
        fallback_used: (dispatch.channel == SubmitChannel::Fallback).then_some(false),
    };
    if finish(
        db,
        lease,
        &change,
        "submit_rate_limited",
        dispatch.with_step(json!({
            "message": message,
            "next_poll_at": next.to_rfc3339(),
            "retry_after_source": retry_after_source,
            "channel": dispatch.channel.as_str(),
        })),
    )? {
        warn!(job_id = %lease.job.id, retry_after_source, "submit rate limited; next attempt scheduled");
    }
    Ok(())
}

/// The provider provably refused the submission for a reason an operator must
/// fix first. The job keeps no receipt, so `retry` resubmits it afterwards.
fn park_submit_needs_manual(
    config: &Config,
    db: &Db,
    lease: &Lease,
    dispatch: Dispatch<'_>,
    err: BackendError,
) -> Result<()> {
    let reason = err.to_string();
    let change = JobChange {
        status: JobStatus::FailedNeedsManual,
        attempt: Some(lease.job.attempt + 1),
        next_poll_at: Some(None),
        last_error: Some(Some(reason.clone())),
        submit_stage: None,
        fallback_used: None,
    };
    if finish(
        db,
        lease,
        &change,
        "submit_failed_needs_manual",
        dispatch.with_step(json!({ "reason": reason, "channel": dispatch.channel.as_str() })),
    )? {
        fire_notification(
            &config.notifications,
            NotificationKind::FailedNeedsManual,
            Some(&lease.job.paper_id),
            Some(&lease.job.id),
            Some(&reason),
        );
        error!(job_id = %lease.job.id, "submit refused; manual intervention required");
    }
    Ok(())
}

fn fail_submit(db: &Db, lease: &Lease, dispatch: Dispatch<'_>, err: BackendError) -> Result<()> {
    let reason = err.to_string();
    let change = JobChange {
        status: JobStatus::Failed,
        attempt: Some(lease.job.attempt + 1),
        next_poll_at: Some(None),
        last_error: Some(Some(reason.clone())),
        submit_stage: None,
        fallback_used: None,
    };
    if finish(
        db,
        lease,
        &change,
        "submit_failed",
        dispatch.with_step(json!({ "reason": reason, "channel": dispatch.channel.as_str() })),
    )? {
        error!(job_id = %lease.job.id, "submit failed");
    }
    Ok(())
}

/// Poll one PROCESSING job now, ignoring its schedule (explicit CLI action, or a token
/// that just arrived).
pub async fn poll_job(config: &Config, db: &Db, job_id: &str) -> Result<Attempt> {
    let job = project_job(config, db, job_id)?;
    let backend = build_backend(config, &job.backend, Some(db), Some(&config.project_id))?;
    poll_job_with_backend(config, db, job_id, backend.as_ref()).await
}

/// [`poll_job`] against a caller-supplied backend.
pub async fn poll_job_with_backend(
    config: &Config,
    db: &Db,
    job_id: &str,
    backend: &dyn ReviewBackend,
) -> Result<Attempt> {
    project_job(config, db, job_id)?;
    let Some(lease) = db.claim_job(
        job_id,
        WorkKind::Poll,
        ClaimTiming::Now,
        Utc::now(),
        POLL_LEASE_TTL,
    )?
    else {
        info!(
            job_id,
            "poll skipped: job is not PROCESSING or another worker holds it"
        );
        return Ok(Attempt::NotClaimed);
    };
    poll_leased(config, db, lease, backend).await?;
    Ok(Attempt::Ran)
}

async fn poll_leased(
    config: &Config,
    db: &Db,
    lease: Lease,
    backend: &dyn ReviewBackend,
) -> Result<()> {
    let job = &lease.job;
    // NOTE: span is entered here; context is carried through sync code but
    // not propagated across .await points (pragmatic trade-off over a full
    // async body rewrite — still provides structured context on function entry).
    let _span = tracing::info_span!(
        "poll_job",
        job_id = %job.id,
        paper_id = %job.paper_id,
        attempt = job.attempt
    )
    .entered();

    let Some(token) = job.token.clone() else {
        let err = anyhow::anyhow!("job {} has no token", job.id);
        return Err(abandon_claim(db, &lease, err));
    };

    let fetched = tokio::time::timeout(POLL_CALL_TIMEOUT, backend.fetch_review(&token))
        .await
        .unwrap_or_else(|_| {
            Err(BackendError::Network(format!(
                "no response within {}s",
                POLL_CALL_TIMEOUT.as_secs()
            )))
        });
    let attempt = job.attempt + 1;
    let retry_at = || {
        compute_next_poll_at(
            Utc::now(),
            &config.polling.schedule_minutes,
            attempt,
            config.polling.jitter_percent,
        )
    };
    let still_processing = |next: chrono::DateTime<Utc>, last_error: Option<String>| JobChange {
        status: JobStatus::Processing,
        attempt: Some(attempt),
        next_poll_at: Some(Some(next)),
        last_error: Some(last_error),
        submit_stage: None,
        fallback_used: None,
    };

    match fetched {
        Ok(ReviewFetchResult::Processing) => {
            let next = retry_at();
            finish(
                db,
                &lease,
                &still_processing(next, None),
                "poll_processing",
                json!({ "attempt": attempt, "next_poll_at": next.to_rfc3339() }),
            )?;
        }
        Ok(ReviewFetchResult::Ready { raw_json }) => {
            // Writing artifacts before the ownership check is harmless: a poll lease
            // guards nothing the provider holds, so a lost lease only leaves files for
            // a job that ended another way.
            let source = provider_source(config, &job.backend);
            let summary_md = match write_review_artifacts(
                &config.state_dir(),
                job,
                &token,
                &raw_json,
                &source,
            ) {
                Ok((_, summary_md, _)) => summary_md,
                Err(err) => return Err(abandon_claim(db, &lease, err)),
            };
            let raw_json = raw_json.to_string();
            let change = JobChange {
                status: JobStatus::Completed,
                attempt: Some(attempt),
                next_poll_at: Some(None),
                last_error: Some(None),
                submit_stage: None,
                fallback_used: None,
            };
            let write = db.finish_lease_with_review(
                &lease,
                Utc::now(),
                NewReview {
                    token: &token,
                    raw_json: &raw_json,
                    summary_md: &summary_md,
                },
                &change,
                "review_completed",
                json!({ "token": token }),
            )?;
            if applied(db, &lease, write, "review_completed")? {
                fire_notification(
                    &config.notifications,
                    NotificationKind::Completed,
                    Some(&job.paper_id),
                    Some(&job.id),
                    Some("ready"),
                );
                info!(job_id = %job.id, "review completed and artifacts written");
            }
        }
        Ok(ReviewFetchResult::Failed { reason }) => {
            let detail = format!(
                "provider reported the review failed: {reason}; request a new review with `reviewloop submit --paper-id {} --force`",
                job.paper_id
            );
            let change = JobChange {
                status: JobStatus::FailedNeedsManual,
                attempt: Some(attempt),
                next_poll_at: Some(None),
                last_error: Some(Some(detail.clone())),
                submit_stage: None,
                fallback_used: None,
            };
            if finish(
                db,
                &lease,
                &change,
                "poll_provider_failed",
                json!({ "reason": reason }),
            )? {
                fire_notification(
                    &config.notifications,
                    NotificationKind::FailedNeedsManual,
                    Some(&job.paper_id),
                    Some(&job.id),
                    Some(&detail),
                );
                warn!(job_id = %job.id, "provider reported the review failed; marked failed-needs-manual");
            }
        }
        Ok(ReviewFetchResult::InvalidToken) => {
            let change = JobChange {
                status: JobStatus::Failed,
                attempt: Some(attempt),
                next_poll_at: Some(None),
                last_error: Some(Some("invalid token".to_string())),
                submit_stage: None,
                fallback_used: None,
            };
            if finish(
                db,
                &lease,
                &change,
                "invalid_token",
                json!({ "token": token }),
            )? {
                warn!(job_id = %job.id, "invalid token reported by backend");
            }
        }
        Err(BackendError::RateLimited {
            message,
            retry_after,
        }) => {
            let next = match retry_after {
                Some(d) => Utc::now() + d,
                None => retry_at(),
            };
            let retry_after_source = if retry_after.is_some() {
                "server"
            } else {
                "schedule"
            };
            if finish(
                db,
                &lease,
                &still_processing(next, Some(message.clone())),
                "poll_rate_limited",
                json!({ "message": message, "next_poll_at": next.to_rfc3339(), "retry_after_source": retry_after_source }),
            )? {
                warn!(job_id = %job.id, retry_after_source, "poll rate limited; next attempt scheduled");
            }
        }
        Err(BackendError::Server { status, body })
            if is_terminal_review_generation_failure(&body) =>
        {
            let reason = format!("terminal backend error ({status}): {body}");
            let change = JobChange {
                status: JobStatus::FailedNeedsManual,
                attempt: Some(attempt),
                next_poll_at: Some(None),
                last_error: Some(Some(reason.clone())),
                submit_stage: None,
                fallback_used: None,
            };
            if finish(
                db,
                &lease,
                &change,
                "poll_terminal_error",
                json!({ "status": status, "message": body }),
            )? {
                fire_notification(
                    &config.notifications,
                    NotificationKind::FailedNeedsManual,
                    Some(&job.paper_id),
                    Some(&job.id),
                    Some(&reason),
                );
                warn!(
                    job_id = %job.id,
                    status,
                    "poll returned terminal review-generation failure; marked failed-needs-manual"
                );
            }
        }
        Err(BackendError::Server { status, body }) => {
            let next = retry_at();
            if finish(
                db,
                &lease,
                &still_processing(next, Some(body.clone())),
                "poll_server_error",
                json!({ "status": status, "message": body, "next_poll_at": next.to_rfc3339(), "retry_after_source": "schedule" }),
            )? {
                warn!(job_id = %job.id, "poll server error; scheduled retry via polling cadence");
            }
        }
        Err(err) => {
            let next = retry_at();
            if finish(
                db,
                &lease,
                &still_processing(next, Some(err.to_string())),
                "poll_error",
                json!({ "error": err.to_string(), "next_poll_at": next.to_rfc3339() }),
            )? {
                warn!(job_id = %job.id, "poll failed; scheduled retry");
            }
        }
    }

    Ok(())
}

fn project_job(config: &Config, db: &Db, job_id: &str) -> Result<Job> {
    let job = db
        .get_job(job_id)?
        .with_context(|| format!("job not found: {job_id}"))?;
    if job.project_id != config.project_id {
        anyhow::bail!(
            "job {} belongs to project {} not current project {}",
            job.id,
            job.project_id,
            config.project_id
        );
    }
    Ok(job)
}

/// Apply a lease owner's result. Returns `false` when the lease was lost — the job was
/// cancelled, overridden, or taken over — in which case nothing was written.
fn finish(
    db: &Db,
    lease: &Lease,
    change: &JobChange,
    event_type: &str,
    payload: Value,
) -> Result<bool> {
    let write = db.finish_lease(lease, Utc::now(), change, event_type, payload)?;
    applied(db, lease, write, event_type)
}

fn applied(db: &Db, lease: &Lease, write: LeaseWrite, outcome: &str) -> Result<bool> {
    let LeaseWrite::Lost(current) = write else {
        return Ok(true);
    };
    let current = current.map(JobStatus::as_str);
    warn!(
        job_id = %lease.job.id,
        kind = lease.kind.as_str(),
        outcome,
        current_status = ?current,
        "lease lost; stale worker result rejected"
    );
    db.add_event(
        None,
        Some(&lease.job.id),
        "stale_result_rejected",
        json!({
            "kind": lease.kind.as_str(),
            "owner": lease.owner,
            "outcome": outcome,
            "current_status": current,
        }),
    )?;
    Ok(false)
}

/// Release a claim after a local error, before anything was sent, and hand back the
/// error. If the release itself fails the claim simply expires and is recovered.
fn abandon_claim(db: &Db, lease: &Lease, err: anyhow::Error) -> anyhow::Error {
    if let Err(release_err) = db.release_lease(lease) {
        warn!(
            job_id = %lease.job.id,
            error = %release_err,
            "failed to release claim after local error; it will be recovered when the lease expires"
        );
    }
    err
}

pub fn mark_timeouts(config: &Config, db: &Db) -> Result<()> {
    let now = Utc::now();

    for job in db.list_processing_jobs(&config.project_id)? {
        let timeout = review_timeout(config);
        let reference_start = job.started_at.unwrap_or(job.created_at);
        if now - reference_start < timeout {
            continue;
        }
        // Claiming first lets a poll in flight finish; if the job is still PROCESSING
        // afterwards, the next tick times it out.
        let Some(lease) = db.claim_job(
            &job.id,
            WorkKind::Poll,
            ClaimTiming::Now,
            now,
            POLL_LEASE_TTL,
        )?
        else {
            continue;
        };
        let change = JobChange {
            status: JobStatus::Timeout,
            attempt: Some(job.attempt),
            next_poll_at: Some(None),
            last_error: Some(Some("review timed out".to_string())),
            submit_stage: None,
            fallback_used: None,
        };
        if finish(db, &lease, &change, "timeout", json!({}))? {
            fire_notification(
                &config.notifications,
                NotificationKind::Timeout,
                Some(&job.paper_id),
                Some(&job.id),
                None,
            );
            warn!(job_id = %job.id, "job timed out");
        }
    }

    Ok(())
}

/// How long a review may take. The provider says processing time follows its load
/// ("hours or even longer"), not the paper's length, so every job gets the configured
/// timeout; a premature TIMEOUT is terminal, while a long one only delays noticing a
/// stuck job.
fn review_timeout(config: &Config) -> Duration {
    Duration::hours(i64::max(config.core.review_timeout_hours as i64, 1))
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
    // After job rows are pruned, so their snapshots become unreferenced.
    let snapshots = prune_unreferenced_snapshots(
        &config.state_dir(),
        &db.list_job_pdf_hashes()?,
        SNAPSHOT_GC_GRACE,
    )?;
    if report.total_deleted() + snapshots == 0 {
        return Ok(());
    }

    db.add_event(
        None,
        None,
        "retention_pruned",
        json!({
            "email_tokens": report.email_tokens,
            "seen_tags": report.seen_tags,
            "events": report.events,
            "reviews": report.reviews,
            "jobs": report.jobs,
            "snapshots": snapshots
        }),
    )?;
    info!(
        deleted = report.total_deleted() + snapshots,
        email_tokens = report.email_tokens,
        seen_tags = report.seen_tags,
        events = report.events,
        reviews = report.reviews,
        jobs = report.jobs,
        snapshots,
        "retention pruning deleted stale records"
    );
    Ok(())
}
