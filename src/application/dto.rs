use super::{ops::CANCELLED_BY_USER, redact::redact_text};
use crate::model::{ExistingReason, Job, JobStatus, NewJob};
use chrono::{DateTime, Utc};
use serde::{Serialize, Serializer};
use serde_json::Value;

/// Serialize a [`JobStatus`] as its database spelling (`"PENDING_APPROVAL"`),
/// not the variant name its derive would emit.
fn status_str<S: Serializer>(status: &JobStatus, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(status.as_str())
}

/// Where a job stands, folded from its status for callers that only need to
/// know what happens next.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobPhase {
    /// PENDING_APPROVAL: waits for `approve_job`; nothing will be sent.
    AwaitingApproval,
    /// QUEUED: stored locally, not yet accepted by the provider. A worker
    /// submits it when `next_poll_at` is due (or at its next tick when null).
    Queued,
    /// PROCESSING: the provider accepted the PDF; the worker polls for the
    /// review at `next_poll_at`. Also the legacy SUBMITTED status, which no
    /// current worker writes or resumes.
    Submitted,
    /// COMPLETED: the review is stored.
    Completed,
    /// FAILED, FAILED_NEEDS_MANUAL or TIMEOUT.
    Failed,
    /// FAILED by `cancel_job`.
    Cancelled,
}

impl JobPhase {
    pub fn of(status: JobStatus, last_error: Option<&str>) -> Self {
        match status {
            JobStatus::PendingApproval => JobPhase::AwaitingApproval,
            JobStatus::Queued => JobPhase::Queued,
            JobStatus::Submitted | JobStatus::Processing => JobPhase::Submitted,
            JobStatus::Completed => JobPhase::Completed,
            JobStatus::Failed if last_error.is_some_and(is_cancellation) => JobPhase::Cancelled,
            JobStatus::Failed | JobStatus::FailedNeedsManual | JobStatus::Timeout => {
                JobPhase::Failed
            }
        }
    }
}

fn is_cancellation(last_error: &str) -> bool {
    last_error
        .strip_prefix(CANCELLED_BY_USER)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with(':'))
}

/// A review job without its provider token or submitter email.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct JobView {
    pub job_id: String,
    pub project_id: String,
    pub paper_id: String,
    pub backend: String,
    #[serde(serialize_with = "status_str")]
    pub status: JobStatus,
    pub phase: JobPhase,
    pub terminal: bool,
    /// The provider acknowledged a submission and returned a review token. The
    /// token itself is never part of a view.
    pub has_token: bool,
    pub review_available: bool,
    pub review_completed_at: Option<DateTime<Utc>>,
    pub attempt: u32,
    /// The PDF path recorded when the job was created.
    pub pdf_path: String,
    /// SHA-256 of the PDF bytes hashed when the job was created.
    pub pdf_hash: String,
    pub venue: Option<String>,
    pub version_no: u32,
    pub round_no: u32,
    pub version_source: String,
    pub version_key: String,
    pub git_tag: Option<String>,
    pub git_commit: Option<String>,
    pub fallback_used: bool,
    /// The last failure, with the job's token replaced by `[redacted]`.
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    /// QUEUED: earliest submission attempt. SUBMITTED/PROCESSING: next
    /// provider poll. Null: no wait; a worker acts at its next tick.
    pub next_poll_at: Option<DateTime<Utc>>,
}

impl JobView {
    pub(crate) fn new(job: &Job, review_completed_at: Option<DateTime<Utc>>) -> Self {
        let tokens: Vec<&str> = job.token.as_deref().into_iter().collect();
        JobView {
            job_id: job.id.clone(),
            project_id: job.project_id.clone(),
            paper_id: job.paper_id.clone(),
            backend: job.backend.clone(),
            status: job.status,
            phase: JobPhase::of(job.status, job.last_error.as_deref()),
            terminal: job.status.is_terminal(),
            has_token: job.token.is_some(),
            review_available: review_completed_at.is_some(),
            review_completed_at,
            attempt: job.attempt,
            pdf_path: job.pdf_path.clone(),
            pdf_hash: job.pdf_hash.clone(),
            venue: job.venue.clone(),
            version_no: job.version_no,
            round_no: job.round_no,
            version_source: job.version_source.clone(),
            version_key: job.version_key.clone(),
            git_tag: job.git_tag.clone(),
            git_commit: job.git_commit.clone(),
            fallback_used: job.fallback_used,
            last_error: job
                .last_error
                .as_deref()
                .map(|error| redact_text(error, &tokens)),
            created_at: job.created_at,
            updated_at: job.updated_at,
            started_at: job.started_at,
            next_poll_at: job.next_poll_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct JobList {
    pub jobs: Vec<JobView>,
    /// More jobs matched than `limit`; only the newest are returned.
    pub truncated: bool,
}

/// A job named by a paper reference that matched more than one job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct JobCandidate {
    pub job_id: String,
    #[serde(serialize_with = "status_str")]
    pub status: JobStatus,
}

/// A project from the registry the CLI keeps of every `reviewloop.toml` it
/// has loaded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProjectView {
    pub project_id: String,
    pub config_path: String,
    /// The registered `reviewloop.toml` still exists.
    pub config_present: bool,
    pub last_seen_at: DateTime<Utc>,
    /// This is the project the operations were called for.
    pub current: bool,
}

/// A paper configured in the project's `reviewloop.toml`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PaperView {
    pub paper_id: String,
    pub backend: String,
    /// The venue a new request would use (paper, then provider default).
    pub venue: Option<String>,
    pub pdf_path: String,
    pub pdf_present: bool,
    pub watched: bool,
    pub tag_trigger: Option<String>,
}

/// What a request asked to review: the paper's current PDF and the review
/// identity that decides coverage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ManuscriptInput {
    pub paper_id: String,
    pub pdf_path: String,
    pub pdf_hash: String,
    pub backend: String,
    pub venue: Option<String>,
    pub version_source: String,
    pub version_key: String,
}

impl ManuscriptInput {
    pub(crate) fn new(job: &NewJob) -> Self {
        let identity = job.review_identity();
        ManuscriptInput {
            paper_id: identity.paper_id,
            pdf_path: job.pdf_path.clone(),
            pdf_hash: identity.pdf_hash,
            backend: identity.backend,
            venue: identity.venue,
            version_source: identity.version_source.as_str().to_string(),
            version_key: identity.version_key,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestDisposition {
    /// A new job was stored.
    Created,
    /// The request key was already bound to a job, or a pending, in-flight or
    /// completed job covers the same review identity; that job is returned
    /// and no job was stored (`reason` says which).
    Existing,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ReviewRequestOutcome {
    pub disposition: RequestDisposition,
    /// Why an `existing` job was returned: `request_replay` or `covered`.
    /// Null when a job was created.
    pub reason: Option<ExistingReason>,
    pub job: JobView,
    pub input: ManuscriptInput,
}

/// The job after an approve or cancel, with the status it left.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TransitionOutcome {
    pub job: JobView,
    #[serde(serialize_with = "status_str")]
    pub previous_status: JobStatus,
}

/// What a retry left for the worker (or an immediate caller) to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryAction {
    /// PROCESSING with a token; polled when the first schedule step is due.
    PollScheduled,
    /// QUEUED without a token; submitted at the worker's next tick.
    SubmissionQueued,
    /// Forced: PROCESSING with a token, due for a poll now.
    PollNow,
    /// Forced: QUEUED without a token, due for submission now.
    SubmitNow,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RetryOutcome {
    pub job: JobView,
    #[serde(serialize_with = "status_str")]
    pub previous_status: JobStatus,
    pub action: RetryAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReviewSection {
    pub name: String,
    pub text: String,
}

/// Review files on disk. `meta.json` is never listed: it stores the token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReviewArtifacts {
    pub dir: Option<String>,
    pub review_md: Option<String>,
    pub review_json: Option<String>,
}

/// A stored review. `markdown`, `section` and `raw` are filled according to
/// the requested part; every text is token-redacted.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ReviewView {
    pub job: JobView,
    pub completed_at: DateTime<Utc>,
    pub score: Option<String>,
    pub title: Option<String>,
    pub sections: Vec<String>,
    pub markdown: Option<String>,
    pub section: Option<ReviewSection>,
    pub raw: Option<Value>,
    pub artifacts: ReviewArtifacts,
}
