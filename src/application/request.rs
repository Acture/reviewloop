use crate::model::JobStatus;

/// How a request names a job: by id, or by paper. A paper reference resolves
/// to the single job of that paper the operation may act on (see
/// [`Eligibility`]); zero or several candidates are errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobRef {
    Id(String),
    Paper(String),
}

/// The job statuses a paper reference may resolve to for one action. `action`
/// names the action in error messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Eligibility {
    pub action: &'static str,
    pub statuses: &'static [JobStatus],
}

impl Eligibility {
    pub const APPROVE: Self = Self {
        action: "approve",
        statuses: &[JobStatus::PendingApproval],
    };
    pub const CANCEL: Self = Self {
        action: "cancel",
        statuses: &[
            JobStatus::PendingApproval,
            JobStatus::Queued,
            JobStatus::Submitted,
            JobStatus::Processing,
        ],
    };
    pub const RETRY_ACTIVE: Self = Self {
        action: "retry",
        statuses: &[
            JobStatus::Queued,
            JobStatus::Submitted,
            JobStatus::Processing,
        ],
    };
    pub const RETRY_INCLUDING_FAILED: Self = Self {
        action: "retry",
        statuses: &[
            JobStatus::Queued,
            JobStatus::Submitted,
            JobStatus::Processing,
            JobStatus::Failed,
            JobStatus::FailedNeedsManual,
            JobStatus::Timeout,
        ],
    };

    pub fn retry(include_failed: bool) -> Self {
        if include_failed {
            Self::RETRY_INCLUDING_FAILED
        } else {
            Self::RETRY_ACTIVE
        }
    }
}

/// Who asked for a review. Recorded as the `source` of the enqueue event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestOrigin {
    /// `reviewloop submit` and `paper add --submit-now`.
    Submit,
    /// `reviewloop run`.
    Run,
    /// An agent through the MCP adapter.
    Agent,
}

impl RequestOrigin {
    /// `source` of the `job_enqueued` / `duplicate_skipped` event.
    pub(crate) fn source(self) -> &'static str {
        match self {
            RequestOrigin::Submit => "manual_submit",
            RequestOrigin::Run => "run",
            RequestOrigin::Agent => "agent_request",
        }
    }

    /// `from_command` of the `force_clear_cooldown` event.
    pub(crate) fn force_label(self) -> &'static str {
        match self {
            RequestOrigin::Submit | RequestOrigin::Run => "submit --force",
            RequestOrigin::Agent => "request_review force",
        }
    }
}

/// Whether a new job may go to the provider without a separate approval.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Approval {
    /// The request itself is the approval: the job starts QUEUED.
    Granted,
    /// The job starts PENDING_APPROVAL and waits for `approve_job`.
    Required,
}

/// Enqueue a review of a configured paper's current PDF.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewRequest {
    pub paper_id: String,
    /// Idempotency key, scoped to the project. Replaying it returns the job it
    /// first resolved to, even once that job finished, until retention prunes
    /// it, provided the review identity derived from the paper's current PDF
    /// and config still matches; otherwise the replay is
    /// [`OpError::RequestConflict`]. A new review round needs a new key.
    ///
    /// [`OpError::RequestConflict`]: super::OpError::RequestConflict
    pub request_key: Option<String>,
    /// Start a new review round even when a job already covers this
    /// manuscript, and clear the cooldown of the paper's other active jobs.
    pub force: bool,
    pub approval: Approval,
    pub origin: RequestOrigin,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JobListQuery {
    pub paper_id: Option<String>,
    /// Keep only PENDING_APPROVAL, QUEUED, SUBMITTED and PROCESSING jobs.
    pub active_only: bool,
    /// Defaults to [`DEFAULT_JOB_LIST_LIMIT`](super::DEFAULT_JOB_LIST_LIMIT);
    /// capped at [`MAX_JOB_LIST_LIMIT`](super::MAX_JOB_LIST_LIMIT).
    pub limit: Option<usize>,
}

/// Which part of a stored review to return. Metadata and section names are
/// always returned.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ReviewPart {
    /// Metadata and section names only.
    Summary,
    /// The rendered review markdown.
    #[default]
    Markdown,
    /// One section's text.
    Section(String),
    /// The provider's review JSON, token-redacted.
    Raw,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewQuery {
    pub job_id: String,
    pub part: ReviewPart,
}

/// Re-queue a job for another submission or poll attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryRequest {
    pub job: JobRef,
    /// Make the job due now instead of following the polling schedule. The
    /// caller may then submit or poll it immediately; otherwise the worker
    /// does at its next tick.
    pub force: bool,
    /// Let a paper reference also match FAILED, FAILED_NEEDS_MANUAL and
    /// TIMEOUT jobs.
    pub include_failed: bool,
    /// With `force`, the caller submits or polls the job itself right away
    /// (`reviewloop retry --force`). A forced poll then leaves `next_poll_at`
    /// alone so a worker does not poll the same job concurrently.
    pub caller_executes: bool,
}

/// Cancel a non-terminal job locally. The provider is not contacted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelRequest {
    pub job: JobRef,
    pub reason: Option<String>,
}
