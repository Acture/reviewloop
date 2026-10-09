use super::{
    dto::{
        JobCandidate, JobList, JobView, ManuscriptInput, PaperView, ProjectView,
        RequestDisposition, RetryAction, RetryOutcome, ReviewArtifacts, ReviewRequestOutcome,
        ReviewSection, ReviewView, TransitionOutcome,
    },
    error::OpError,
    operation::Operation,
    redact::redact_value,
    request::{
        Approval, CancelRequest, Eligibility, JobListQuery, JobRef, RetryRequest, ReviewPart,
        ReviewQuery, ReviewRequest,
    },
};
use crate::{
    artifact::render_summary_markdown,
    backend::input::{InputVerdict, input_policy},
    config::{Config, PaperConfig},
    db::{CancelOutcome, Db, Requeue},
    email_account::resolve_submission_email,
    model::{
        EnqueueConflict, EnqueueMode, EnqueueOutcome, EnqueueRequest, Job, JobPdf, JobStatus,
        NewJob,
    },
    submission_input::prepare_input,
    util::compute_next_poll_at,
};
use chrono::Utc;
use serde_json::{Value, json};
use std::path::Path;
use tracing::info;

/// Settings the paper's provider needs before a request can be submitted.
/// Checked at enqueue, like the Stanford submitter email, so a job that could
/// only fail is never queued.
fn check_provider_settings(config: &Config, paper: &PaperConfig) -> Result<(), OpError> {
    match config.missing_provider_setting(paper) {
        Some((setting, message)) => Err(OpError::ProviderNotConfigured {
            backend: paper.backend.clone(),
            setting,
            message,
        }),
        None => Ok(()),
    }
}

/// `last_error` prefix of a cancelled job. The widget and the failure lists
/// in [`Db`] filter on it.
pub const CANCELLED_BY_USER: &str = "cancelled by user";
pub const DEFAULT_JOB_LIST_LIMIT: usize = 50;
pub const MAX_JOB_LIST_LIMIT: usize = 200;

/// Section order of `render_summary_markdown`; other sections follow by name.
const KNOWN_SECTIONS: [&str; 7] = [
    "summary",
    "strengths",
    "weaknesses",
    "detailed_comments",
    "questions",
    "assessment",
    "full_review",
];

/// The config's `project_id`, or [`OpError::ProjectRequired`] when it is
/// empty.
pub fn require_project(config: &Config) -> Result<&str, OpError> {
    let project_id = config.project_id.as_str();
    if project_id.trim().is_empty() {
        return Err(OpError::ProjectRequired);
    }
    Ok(project_id)
}

/// The review operations for one project: its loaded config and the database.
#[derive(Debug, Clone, Copy)]
pub struct ReviewOps<'a> {
    config: &'a Config,
    db: &'a Db,
}

impl<'a> ReviewOps<'a> {
    pub fn new(config: &'a Config, db: &'a Db) -> Self {
        Self { config, db }
    }

    /// Every project in the registry.
    pub fn list_projects(&self) -> Result<Vec<ProjectView>, OpError> {
        Ok(self
            .db
            .list_registered_projects()?
            .into_iter()
            .map(|project| ProjectView {
                current: project.project_id == self.config.project_id,
                config_present: project.config_path.exists(),
                config_path: project.config_path.display().to_string(),
                last_seen_at: project.last_seen_at,
                project_id: project.project_id,
            })
            .collect())
    }

    /// The papers configured for this project.
    pub fn list_papers(&self) -> Result<Vec<PaperView>, OpError> {
        require_project(self.config)?;
        Ok(self
            .config
            .papers
            .iter()
            .map(|paper| PaperView {
                paper_id: paper.id.clone(),
                backend: paper.backend.clone(),
                venue: self.config.venue_for(paper),
                review_options: self.config.review_options_for(paper),
                pdf_path: paper.pdf_path.clone(),
                pdf_present: Path::new(&paper.pdf_path).exists(),
                watched: self.config.is_paper_watched(&paper.id),
                tag_trigger: self.config.paper_tag_trigger(&paper.id).map(str::to_string),
            })
            .collect())
    }

    /// Snapshot the paper's current PDF and enqueue a review of it through
    /// [`Db::enqueue`]. The job is only stored: it is QUEUED (or
    /// PENDING_APPROVAL), not submitted,
    /// until a worker or the caller submits it. A replayed `request_key`
    /// returns the job it first resolved to; unless `force`, a pending,
    /// in-flight or completed job with the same review identity is returned
    /// instead of a new one.
    pub fn request_review(&self, request: &ReviewRequest) -> Result<ReviewRequestOutcome, OpError> {
        let project_id = require_project(self.config)?;
        if request
            .request_key
            .as_deref()
            .is_some_and(|key| key.trim().is_empty())
        {
            return Err(OpError::InvalidRequest {
                field: "request_key",
                message: "request key must not be blank".to_string(),
            });
        }
        let paper = self
            .config
            .find_paper(&request.paper_id)
            .ok_or_else(|| OpError::paper_not_found(&request.paper_id, self.config))?;
        let pdf_path = Path::new(&paper.pdf_path);
        if !pdf_path.exists() {
            return Err(OpError::PdfNotFound {
                paper_id: paper.id.clone(),
                path: pdf_path.display().to_string(),
            });
        }

        check_provider_settings(self.config, paper)?;
        let notices = match input_policy(&paper.backend) {
            Some(policy) => match policy.check(pdf_path)? {
                InputVerdict::Accepted { notices, .. } => notices,
                InputVerdict::Rejected { reason } => {
                    return Err(OpError::InputRejected {
                        paper_id: paper.id.clone(),
                        backend: paper.backend.clone(),
                        reason,
                    });
                }
            },
            None => Vec::new(),
        };

        let email = if paper.backend == "stanford" {
            resolve_submission_email(self.config, "stanford", None).map_err(|err| {
                OpError::SubmitterEmailUnavailable {
                    backend: paper.backend.clone(),
                    detail: format!("{err:#}"),
                }
            })?
        } else {
            String::new()
        };
        let job = NewJob {
            project_id: project_id.to_string(),
            paper_id: paper.id.clone(),
            backend: paper.backend.clone(),
            // Pinned before enqueueing, so coverage and the request key see the
            // bytes every submission of the job will upload.
            pdf: JobPdf::Pinned(prepare_input(&self.config.state_dir(), pdf_path)?),
            status: match request.approval {
                Approval::Granted => JobStatus::Queued,
                Approval::Required => JobStatus::PendingApproval,
            },
            email,
            venue: self.config.venue_for(paper),
            review_options: self.config.review_options_for(paper),
            git_tag: None,
            git_commit: None,
            next_poll_at: None,
        };
        let input = ManuscriptInput::new(&job, notices);

        let outcome = self
            .db
            .enqueue(&EnqueueRequest {
                job,
                request_key: request.request_key.clone(),
                mode: if request.force {
                    EnqueueMode::NewRound
                } else {
                    EnqueueMode::Deduplicate
                },
                source: request.origin.source().to_string(),
            })
            .map_err(|err| match err.downcast::<EnqueueConflict>() {
                Ok(conflict) => OpError::RequestConflict(conflict),
                Err(other) => OpError::Internal(other),
            })?;
        let (job, reason) = match outcome {
            EnqueueOutcome::Created(job) => (job, None),
            EnqueueOutcome::Existing { job, reason } => (job, Some(reason)),
        };
        if reason.is_none() {
            if request.force {
                self.clear_sibling_cooldowns(&job, request)?;
            }
            info!(
                project_id,
                paper_id = %job.paper_id,
                job_id = %job.id,
                status = job.status.as_str(),
                "review request enqueued"
            );
        }

        Ok(ReviewRequestOutcome {
            disposition: match reason {
                None => RequestDisposition::Created,
                Some(_) => RequestDisposition::Existing,
            },
            reason,
            job: self.view(&job)?,
            input,
        })
    }

    /// Resolve a job reference. An id is looked up in this project (in every
    /// project when unscoped); a paper reference needs a project and must
    /// match exactly one job in `eligibility`'s statuses, newest first.
    pub fn find_job(&self, job: &JobRef, eligibility: Eligibility) -> Result<Job, OpError> {
        match job {
            JobRef::Id(job_id) => self.job_by_id(job_id),
            JobRef::Paper(paper_id) => {
                let project_id = require_project(self.config)?;
                self.paper_job(project_id, paper_id, eligibility)
            }
        }
    }

    /// Read one job. Database only: this never polls the provider.
    pub fn get_job(&self, job_id: &str) -> Result<JobView, OpError> {
        self.view(&self.job_by_id(job_id)?)
    }

    /// This project's jobs, newest first.
    pub fn list_jobs(&self, query: &JobListQuery) -> Result<JobList, OpError> {
        let project_id = require_project(self.config)?;
        let limit = query
            .limit
            .unwrap_or(DEFAULT_JOB_LIST_LIMIT)
            .clamp(1, MAX_JOB_LIST_LIMIT);
        let mut rows = self.db.list_project_jobs(
            project_id,
            query.paper_id.as_deref(),
            query.active_only,
            limit + 1,
        )?;
        let truncated = rows.len() > limit;
        rows.truncate(limit);
        Ok(JobList {
            jobs: rows
                .iter()
                .map(|(job, review_completed_at)| JobView::new(job, *review_completed_at))
                .collect(),
            truncated,
        })
    }

    /// Read a stored review.
    pub fn get_review(&self, query: &ReviewQuery) -> Result<ReviewView, OpError> {
        let job = self.job_by_id(&query.job_id)?;
        let Some(record) = self.db.get_review(&job.id)? else {
            return Err(OpError::ReviewNotAvailable {
                job_id: job.id,
                status: job.status,
            });
        };
        // The review may have been fetched with an earlier token than the
        // job's current one; hide both.
        let tokens: Vec<&str> = job
            .token
            .as_deref()
            .into_iter()
            .chain([record.token.as_str()])
            .collect();
        let raw = redact_value(record.raw_json, &tokens);
        let sections = section_names(&raw);

        let mut view = ReviewView {
            job: JobView::new(&job, Some(record.completed_at)),
            completed_at: record.completed_at,
            score: score_of(&raw),
            title: raw.get("title").and_then(Value::as_str).map(str::to_string),
            sections,
            markdown: None,
            section: None,
            raw: None,
            artifacts: self.artifacts(&job.id),
        };
        match &query.part {
            ReviewPart::Summary => {}
            // Rendered from the redacted JSON, which is how the stored
            // markdown was rendered from the original.
            ReviewPart::Markdown => view.markdown = Some(render_summary_markdown(&raw)),
            ReviewPart::Section(name) => {
                let text = section_text(&raw, name).ok_or_else(|| OpError::SectionNotFound {
                    job_id: job.id.clone(),
                    section: name.clone(),
                    available: view.sections.clone(),
                })?;
                view.section = Some(ReviewSection {
                    name: name.clone(),
                    text: text.to_string(),
                });
            }
            ReviewPart::Raw => view.raw = Some(raw),
        }
        Ok(view)
    }

    /// Move a PENDING_APPROVAL job to QUEUED.
    pub fn approve_job(&self, job: &JobRef) -> Result<TransitionOutcome, OpError> {
        require_project(self.config)?;
        let job = self.find_job(job, Eligibility::APPROVE)?;
        if job.status != JobStatus::PendingApproval {
            return Err(OpError::InvalidState {
                message: format!(
                    "job {} is in status {}, only PENDING_APPROVAL can be approved",
                    job.id,
                    job.status.as_str()
                ),
                job_id: job.id,
                status: job.status,
                operation: Operation::ApproveJob,
            });
        }

        self.db
            .update_job_state(&job.id, JobStatus::Queued, None, Some(None), Some(None))?;
        self.db
            .add_event(None, Some(&job.id), "approved", json!({}))?;
        info!(job_id = %job.id, paper_id = %job.paper_id, "job approved");
        self.transitioned(&job)
    }

    /// Re-queue a job: a token-backed job goes back to polling, a tokenless
    /// one back to submission. Every status but PENDING_APPROVAL may be
    /// retried (that one needs `approve_job`); `force` is limited to the
    /// statuses an immediate submit or poll can act on. Uses this config's
    /// polling schedule, so the config must be the job's project.
    pub fn retry_job(&self, request: &RetryRequest) -> Result<RetryOutcome, OpError> {
        let job = self.find_job(&request.job, Eligibility::retry(request.include_failed))?;
        if job.project_id != self.config.project_id {
            return Err(OpError::ProjectMismatch {
                job_id: job.id,
                job_project_id: job.project_id,
                context_project_id: self.config.project_id.clone(),
            });
        }
        if job.status == JobStatus::PendingApproval {
            let message = format!(
                "job {} is in status PENDING_APPROVAL; approve it instead of retrying",
                job.id
            );
            return Err(invalid_retry(&job, &message));
        }

        let action = match (request.force, job.token.is_some()) {
            (true, true) => {
                if job.status != JobStatus::Processing {
                    return Err(invalid_retry(
                        &job,
                        "--force for token-backed jobs only supports PROCESSING jobs",
                    ));
                }
                // A caller polling right away must not also make the job due
                // for a worker; poll_job reschedules it either way.
                if !request.caller_executes {
                    self.db.update_job_state(
                        &job.id,
                        JobStatus::Processing,
                        None,
                        Some(Some(Utc::now())),
                        None,
                    )?;
                }
                self.db.add_event(
                    Some(&job.project_id),
                    Some(&job.id),
                    "manual_rate_limit_override",
                    override_payload(&job, "poll"),
                )?;
                RetryAction::PollNow
            }
            (true, false) => {
                if !matches!(
                    job.status,
                    JobStatus::Queued
                        | JobStatus::Submitted
                        | JobStatus::Failed
                        | JobStatus::FailedNeedsManual
                        | JobStatus::Timeout
                ) {
                    return Err(invalid_retry(
                        &job,
                        "--force for tokenless jobs only supports QUEUED/SUBMITTED/FAILED/FAILED_NEEDS_MANUAL/TIMEOUT",
                    ));
                }
                // user override: reset terminal job back to Queued for re-submission.
                self.requeue(&job)?;
                self.db.add_event(
                    Some(&job.project_id),
                    Some(&job.id),
                    "manual_rate_limit_override",
                    override_payload(&job, "submit"),
                )?;
                RetryAction::SubmitNow
            }
            (false, has_token) => {
                let action = if has_token {
                    let next = compute_next_poll_at(
                        Utc::now(),
                        &self.config.polling.schedule_minutes,
                        0,
                        self.config.polling.jitter_percent,
                    );
                    // user override: explicit retry may cross state-machine boundaries.
                    self.db.update_job_state_unchecked(
                        &job.id,
                        JobStatus::Processing,
                        Some(0),
                        Some(Some(next)),
                        Some(None),
                    )?;
                    RetryAction::PollScheduled
                } else {
                    // user override: explicit retry may cross state-machine boundaries.
                    self.requeue(&job)?;
                    RetryAction::SubmissionQueued
                };
                self.db
                    .add_event(Some(&job.project_id), Some(&job.id), "retried", json!({}))?;
                action
            }
        };
        info!(job_id = %job.id, paper_id = %job.paper_id, ?action, "job retried");

        let outcome = self.transitioned(&job)?;
        Ok(RetryOutcome {
            job: outcome.job,
            previous_status: outcome.previous_status,
            action,
        })
    }

    /// Mark a non-terminal job FAILED with a cancellation reason. The provider
    /// is not contacted, so an already accepted submission keeps running
    /// remotely; its result is no longer collected.
    ///
    /// The status check, the write and the `cancelled` event share one transaction that
    /// also revokes any worker lease, so a worker finishing concurrently either lands
    /// first (the cancel is refused as terminal) or has its result rejected.
    pub fn cancel_job(&self, request: &CancelRequest) -> Result<TransitionOutcome, OpError> {
        let job = self.find_job(&request.job, Eligibility::CANCEL)?;
        let status = match self
            .db
            .cancel_job(&job.id, request.reason.as_deref(), Utc::now())?
        {
            CancelOutcome::Cancelled {
                previous_status, ..
            } => previous_status,
            CancelOutcome::AlreadyTerminal(status) => {
                return Err(OpError::InvalidState {
                    message: format!(
                        "job {} is already in terminal status {}; cannot cancel",
                        job.id,
                        status.as_str()
                    ),
                    job_id: job.id,
                    status,
                    operation: Operation::CancelJob,
                });
            }
        };
        info!(job_id = %job.id, paper_id = %job.paper_id, "job cancelled");
        self.transitioned(&Job { status, ..job })
    }

    /// [`Db::requeue`] with its refusals as typed retry errors.
    fn requeue(&self, job: &Job) -> Result<(), OpError> {
        match self.db.requeue(&job.id, Utc::now())? {
            Requeue::Requeued => Ok(()),
            Requeue::InFlight { owner, expires_at } => Err(invalid_retry(
                job,
                &format!(
                    "job {} is being submitted right now (worker {owner} holds it until {}); wait for the outcome or cancel it",
                    job.id,
                    expires_at.to_rfc3339()
                ),
            )),
            Requeue::HasReceipt => Err(invalid_retry(
                job,
                &format!(
                    "job {} already has a submission receipt; run `reviewloop retry --job-id {}` again to poll it",
                    job.id, job.id
                ),
            )),
        }
    }

    fn job_by_id(&self, job_id: &str) -> Result<Job, OpError> {
        let job = if self.config.project_id.trim().is_empty() {
            self.db.get_job(job_id)?
        } else {
            self.db.get_project_job(&self.config.project_id, job_id)?
        };
        job.ok_or_else(|| OpError::JobNotFound {
            job_id: job_id.to_string(),
        })
    }

    fn paper_job(
        &self,
        project_id: &str,
        paper_id: &str,
        eligibility: Eligibility,
    ) -> Result<Job, OpError> {
        let mut matching: Vec<_> = self
            .db
            .list_status_views(project_id, Some(paper_id))?
            .into_iter()
            .filter_map(|view| {
                let status = *eligibility
                    .statuses
                    .iter()
                    .find(|status| status.as_str() == view.status)?;
                Some((
                    view.updated_at,
                    JobCandidate {
                        job_id: view.id,
                        status,
                    },
                ))
            })
            .collect();
        matching.sort_by_key(|(updated_at, _)| std::cmp::Reverse(*updated_at));

        match matching.as_slice() {
            [] => Err(OpError::NoEligibleJob {
                paper_id: paper_id.to_string(),
                action: eligibility.action,
                statuses: eligibility.statuses,
            }),
            [(_, only)] => self
                .db
                .get_project_job(project_id, &only.job_id)?
                .ok_or_else(|| OpError::JobNotFound {
                    job_id: only.job_id.clone(),
                }),
            _ => Err(OpError::AmbiguousJob {
                paper_id: paper_id.to_string(),
                action: eligibility.action,
                candidates: matching
                    .into_iter()
                    .map(|(_, candidate)| candidate)
                    .collect(),
            }),
        }
    }

    fn view(&self, job: &Job) -> Result<JobView, OpError> {
        Ok(JobView::new(job, self.db.review_completed_at(&job.id)?))
    }

    /// The job re-read after a state change, with the status it had before.
    fn transitioned(&self, before: &Job) -> Result<TransitionOutcome, OpError> {
        let after = self
            .db
            .get_job(&before.id)?
            .ok_or_else(|| OpError::JobNotFound {
                job_id: before.id.clone(),
            })?;
        Ok(TransitionOutcome {
            job: self.view(&after)?,
            previous_status: before.status,
        })
    }

    fn artifacts(&self, job_id: &str) -> ReviewArtifacts {
        let dir = self.config.state_dir().join("artifacts").join(job_id);
        let existing = |name: &str| {
            let path = dir.join(name);
            path.exists().then(|| path.display().to_string())
        };
        ReviewArtifacts {
            review_md: existing("review.md"),
            review_json: existing("review.json"),
            dir: dir.exists().then(|| dir.display().to_string()),
        }
    }

    /// Reset `attempt` and clear `next_poll_at` on the paper's other QUEUED,
    /// SUBMITTED and PROCESSING jobs so they do not wait behind their
    /// cooldowns. Only runs after a forced request created `created`, so a
    /// replayed request never clears cooldowns.
    fn clear_sibling_cooldowns(
        &self,
        created: &Job,
        request: &ReviewRequest,
    ) -> Result<(), OpError> {
        let siblings = self
            .db
            .list_active_jobs_for_paper(&created.project_id, &created.paper_id)?;
        for sibling in siblings.into_iter().filter(|job| job.id != created.id) {
            self.db.reschedule(&sibling.id, Some(0), Some(None))?;
            self.db.add_event(
                Some(&created.project_id),
                Some(&sibling.id),
                "force_clear_cooldown",
                json!({
                    "from_command": request.origin.force_label(),
                    "previous_attempt": sibling.attempt,
                    "previous_next_poll_at": sibling.next_poll_at.map(|t| t.to_rfc3339()),
                }),
            )?;
        }
        Ok(())
    }
}

fn invalid_retry(job: &Job, message: &str) -> OpError {
    OpError::InvalidState {
        job_id: job.id.clone(),
        status: job.status,
        operation: Operation::RetryJob,
        message: message.to_string(),
    }
}

fn override_payload(job: &Job, mode: &str) -> Value {
    json!({
        "paper_id": job.paper_id,
        "mode": mode,
        "reason": "manual_override",
        "previous_status": job.status.as_str(),
        "previous_next_poll_at": job.next_poll_at.map(|value| value.to_rfc3339()),
        "version_no": job.version_no,
        "round_no": job.round_no
    })
}

/// `numerical_score` as text, as `Db::list_status_views` reports it.
fn score_of(raw: &Value) -> Option<String> {
    match raw.get("numerical_score")? {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

/// Text sections of the review JSON: `sections.*` strings in rendering order,
/// or a top-level `content` string.
fn section_names(raw: &Value) -> Vec<String> {
    match raw.get("sections").and_then(Value::as_object) {
        Some(sections) => {
            let texts: Vec<&String> = sections
                .iter()
                .filter(|(_, text)| text.is_string())
                .map(|(name, _)| name)
                .collect();
            let known = KNOWN_SECTIONS
                .iter()
                .filter(|name| texts.iter().any(|text| text == name))
                .map(|name| name.to_string());
            let others = texts
                .iter()
                .filter(|name| !KNOWN_SECTIONS.contains(&name.as_str()))
                .map(|name| name.to_string());
            known.chain(others).collect()
        }
        None if raw.get("content").is_some_and(Value::is_string) => vec!["content".to_string()],
        None => Vec::new(),
    }
}

fn section_text<'v>(raw: &'v Value, name: &str) -> Option<&'v str> {
    match raw.get("sections").and_then(Value::as_object) {
        Some(sections) => sections.get(name).and_then(Value::as_str),
        None if name == "content" => raw.get("content").and_then(Value::as_str),
        None => None,
    }
}
