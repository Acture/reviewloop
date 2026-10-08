use crate::submission_input::PreparedInput;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobStatus {
    PendingApproval,
    Queued,
    Submitted,
    Processing,
    Completed,
    Failed,
    FailedNeedsManual,
    Timeout,
}

impl JobStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            JobStatus::PendingApproval => "PENDING_APPROVAL",
            JobStatus::Queued => "QUEUED",
            JobStatus::Submitted => "SUBMITTED",
            JobStatus::Processing => "PROCESSING",
            JobStatus::Completed => "COMPLETED",
            JobStatus::Failed => "FAILED",
            JobStatus::FailedNeedsManual => "FAILED_NEEDS_MANUAL",
            JobStatus::Timeout => "TIMEOUT",
        }
    }

    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "PENDING_APPROVAL" => Some(JobStatus::PendingApproval),
            "QUEUED" => Some(JobStatus::Queued),
            "SUBMITTED" => Some(JobStatus::Submitted),
            "PROCESSING" => Some(JobStatus::Processing),
            "COMPLETED" => Some(JobStatus::Completed),
            "FAILED" => Some(JobStatus::Failed),
            "FAILED_NEEDS_MANUAL" => Some(JobStatus::FailedNeedsManual),
            "TIMEOUT" => Some(JobStatus::Timeout),
            _ => None,
        }
    }

    /// Completed, Failed, FailedNeedsManual and Timeout end a job.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            JobStatus::Completed
                | JobStatus::Failed
                | JobStatus::FailedNeedsManual
                | JobStatus::Timeout
        )
    }

    /// Returns true if a job in `self` is permitted to move to `to`.
    ///
    /// The state machine is intentionally narrow:
    /// - Terminal states (Completed, Failed, FailedNeedsManual, Timeout) are
    ///   absorbing — no outgoing transitions for automated/daemon paths.
    /// - PendingApproval → Queued (via `reviewloop approve`)
    /// - Queued → {Submitted, Processing, Failed, FailedNeedsManual, Timeout}
    /// - Submitted → {Processing, Failed, FailedNeedsManual, Timeout, Queued}
    /// - Processing → {Completed, Failed, FailedNeedsManual, Timeout, Queued}
    /// - Self-transitions (e.g. Processing → Processing on retry-bookkeeping)
    ///   are always allowed; the worker uses them to bump attempt / next_poll_at
    ///   without changing logical state.
    ///
    /// Note: user-initiated CLI overrides (`retry`, `complete`) deliberately
    /// move jobs out of terminal states. Those call sites are intentional and
    /// should NOT be routed through this guard.
    pub fn can_transition(self, to: JobStatus) -> bool {
        use JobStatus::*;
        if self == to {
            return true;
        }
        match (self, to) {
            (Completed | Failed | FailedNeedsManual | Timeout, _) => false,
            (PendingApproval, Queued) => true,
            (PendingApproval, _) => false,
            (Queued, Submitted | Processing | Failed | FailedNeedsManual | Timeout) => true,
            (Submitted, Processing | Failed | FailedNeedsManual | Timeout | Queued) => true,
            (Processing, Completed | Failed | FailedNeedsManual | Timeout | Queued) => true,
            _ => false,
        }
    }
}

/// Persisted stage of a job's current submit attempt (`jobs.submit_stage`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubmitStage {
    /// A worker holds the job's lease; nothing has been sent to the provider.
    Claimed,
    /// The request may have reached the provider; the lease owner awaits the receipt.
    Dispatched,
    /// The provider may have accepted the request but no receipt was saved. Never
    /// resubmitted automatically; awaits reconciliation (token email, `import-token`,
    /// explicit `retry`, or `cancel`).
    Uncertain,
}

impl SubmitStage {
    pub fn as_str(self) -> &'static str {
        match self {
            SubmitStage::Claimed => "CLAIMED",
            SubmitStage::Dispatched => "DISPATCHED",
            SubmitStage::Uncertain => "UNCERTAIN",
        }
    }

    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "CLAIMED" => Some(SubmitStage::Claimed),
            "DISPATCHED" => Some(SubmitStage::Dispatched),
            "UNCERTAIN" => Some(SubmitStage::Uncertain),
            _ => None,
        }
    }
}

/// The work a job lease grants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkKind {
    Submit,
    Poll,
}

impl WorkKind {
    pub fn as_str(self) -> &'static str {
        match self {
            WorkKind::Submit => "submit",
            WorkKind::Poll => "poll",
        }
    }
}

/// Route a submit attempt is dispatched through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitChannel {
    Primary,
    Fallback,
}

impl SubmitChannel {
    pub fn as_str(self) -> &'static str {
        match self {
            SubmitChannel::Primary => "primary",
            SubmitChannel::Fallback => "fallback",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::JobStatus;

    #[test]
    fn terminal_states_are_absorbing() {
        use JobStatus::*;
        let terminals = [Completed, Failed, FailedNeedsManual, Timeout];
        let all = [
            PendingApproval,
            Queued,
            Submitted,
            Processing,
            Completed,
            Failed,
            FailedNeedsManual,
            Timeout,
        ];
        for t in terminals {
            for to in all {
                if t == to {
                    assert!(
                        t.can_transition(to),
                        "{:?} -> {:?} self-transition must be allowed",
                        t,
                        to
                    );
                } else {
                    assert!(
                        !t.can_transition(to),
                        "{:?} -> {:?} must be rejected (terminal absorbing)",
                        t,
                        to
                    );
                }
            }
        }
    }

    #[test]
    fn worker_daemon_transitions_are_allowed() {
        use JobStatus::*;
        // approve command
        assert!(PendingApproval.can_transition(Queued));
        // submit path: Queued -> Processing (direct, skipping Submitted)
        assert!(Queued.can_transition(Processing));
        // submit path failure cases
        assert!(Queued.can_transition(Failed));
        assert!(Queued.can_transition(FailedNeedsManual));
        assert!(Queued.can_transition(Timeout));
        // rate-limit self-transition on submit
        assert!(Queued.can_transition(Queued));
        // poll path
        assert!(Processing.can_transition(Completed));
        assert!(Processing.can_transition(Failed));
        assert!(Processing.can_transition(FailedNeedsManual));
        assert!(Processing.can_transition(Timeout));
        // rate-limit / retry-bookkeeping self-transitions
        assert!(Processing.can_transition(Processing));
        // Submitted fallback transitions
        assert!(Submitted.can_transition(Processing));
        assert!(Submitted.can_transition(Queued));
    }

    #[test]
    fn obviously_invalid_transitions_are_rejected() {
        use JobStatus::*;
        assert!(!Completed.can_transition(Queued));
        assert!(!Completed.can_transition(Processing));
        assert!(!Completed.can_transition(Failed));
        assert!(!Failed.can_transition(Queued));
        assert!(!FailedNeedsManual.can_transition(Queued));
        assert!(!Timeout.can_transition(Queued));
        assert!(!PendingApproval.can_transition(Processing));
        assert!(!PendingApproval.can_transition(Completed));
        assert!(!PendingApproval.can_transition(Failed));
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub project_id: String,
    pub paper_id: String,
    pub backend: String,
    /// Source PDF the job was enqueued from. It may change or disappear after
    /// enqueue; submissions upload `snapshot_path` instead.
    pub pdf_path: String,
    /// SHA-256 of the pinned snapshot bytes.
    pub pdf_hash: String,
    /// Immutable copy uploaded by every submission of this job. `None` only for
    /// jobs created before snapshots existed; the worker backfills it from
    /// `pdf_path` when that file still matches `pdf_hash`.
    pub snapshot_path: Option<String>,
    pub status: JobStatus,
    pub token: Option<String>,
    pub email: String,
    pub venue: Option<String>,
    pub git_tag: Option<String>,
    pub git_commit: Option<String>,
    pub version_no: u32,
    pub round_no: u32,
    pub version_source: String,
    pub version_key: String,
    pub attempt: u32,
    pub started_at: Option<DateTime<Utc>>,
    pub next_poll_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub fallback_used: bool,
    /// Owner of the job's current work lease; ownership lapses at `lease_expires_at`.
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub submit_stage: Option<SubmitStage>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Job {
    /// How an operator settles a submission whose outcome is unknown.
    pub fn reconcile_hint(&self) -> String {
        if self.token.is_some() {
            return format!(
                "a receipt token is saved; run `reviewloop retry --job-id {}` to resume polling",
                self.id
            );
        }
        format!(
            "not resubmitted automatically; once the review email arrives run `reviewloop import-token --job-id {} --token <token>`, or `reviewloop retry --job-id {} --force` to resubmit anyway (may duplicate), or `reviewloop cancel --job-id {}`",
            self.id, self.id, self.id
        )
    }
}

#[derive(Debug, Clone)]
pub struct NewJob {
    pub project_id: String,
    pub paper_id: String,
    pub backend: String,
    pub pdf: JobPdf,
    pub status: JobStatus,
    pub email: String,
    pub venue: Option<String>,
    pub git_tag: Option<String>,
    pub git_commit: Option<String>,
    pub next_poll_at: Option<DateTime<Utc>>,
}

impl NewJob {
    /// The fields that decide what a review means; see [`ReviewIdentity`].
    pub fn review_identity(&self) -> ReviewIdentity {
        ReviewIdentity::new(
            &self.paper_id,
            &self.backend,
            self.pdf.pdf_hash(),
            self.venue.as_deref(),
            self.git_commit.as_deref(),
        )
    }
}

/// The PDF a new job is created for.
#[derive(Debug, Clone)]
pub enum JobPdf {
    /// Snapshot taken at enqueue; every submission uploads exactly these bytes.
    Pinned(PreparedInput),
    /// No snapshot, e.g. a token imported for a submission made outside
    /// reviewloop. If such a job is ever submitted, the worker first snapshots
    /// `pdf_path`, and only when it still hashes to `pdf_hash`.
    Unpinned { pdf_path: String, pdf_hash: String },
}

impl JobPdf {
    pub fn pdf_path(&self) -> String {
        match self {
            JobPdf::Pinned(input) => input.source_path.to_string_lossy().into_owned(),
            JobPdf::Unpinned { pdf_path, .. } => pdf_path.clone(),
        }
    }

    pub fn pdf_hash(&self) -> &str {
        match self {
            JobPdf::Pinned(input) => &input.sha256,
            JobPdf::Unpinned { pdf_hash, .. } => pdf_hash,
        }
    }

    pub fn snapshot_path(&self) -> Option<String> {
        match self {
            JobPdf::Pinned(input) => Some(input.snapshot_path.to_string_lossy().into_owned()),
            JobPdf::Unpinned { .. } => None,
        }
    }
}

/// Where a job's `version_key` came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VersionSource {
    GitCommit,
    PdfHash,
}

impl VersionSource {
    pub fn as_str(self) -> &'static str {
        match self {
            VersionSource::GitCommit => "git_commit",
            VersionSource::PdfHash => "pdf_hash",
        }
    }
}

/// Normalized content of a review request within one project: the manuscript
/// bytes, where and how it is reviewed, and which manuscript version it is.
///
/// Two requests with equal identities ask for the same review, so an active or
/// completed job with this identity covers both. The file path and submitter
/// email are deliberately absent: neither changes what gets reviewed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReviewIdentity {
    pub paper_id: String,
    pub backend: String,
    pub pdf_hash: String,
    /// Trimmed; `None` when unset or blank.
    pub venue: Option<String>,
    pub version_source: VersionSource,
    /// The git commit when known, otherwise the manuscript hash.
    pub version_key: String,
}

impl ReviewIdentity {
    pub fn new(
        paper_id: &str,
        backend: &str,
        pdf_hash: &str,
        venue: Option<&str>,
        git_commit: Option<&str>,
    ) -> Self {
        let commit = git_commit.map(str::trim).filter(|value| !value.is_empty());
        let (version_source, version_key) = match commit {
            Some(commit) => (VersionSource::GitCommit, commit.to_string()),
            None => (VersionSource::PdfHash, pdf_hash.to_string()),
        };
        Self {
            paper_id: paper_id.to_string(),
            backend: backend.to_string(),
            pdf_hash: pdf_hash.to_string(),
            venue: venue
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string),
            version_source,
            version_key,
        }
    }

    /// Fields whose values differ between `self` (the recorded request) and
    /// `requested`, in declaration order.
    pub fn mismatches(&self, requested: &Self) -> Vec<FieldMismatch> {
        let fields: [(&'static str, Option<&str>, Option<&str>); 6] = [
            ("paper_id", Some(&self.paper_id), Some(&requested.paper_id)),
            ("backend", Some(&self.backend), Some(&requested.backend)),
            ("pdf_hash", Some(&self.pdf_hash), Some(&requested.pdf_hash)),
            ("venue", self.venue.as_deref(), requested.venue.as_deref()),
            (
                "version_source",
                Some(self.version_source.as_str()),
                Some(requested.version_source.as_str()),
            ),
            (
                "version_key",
                Some(&self.version_key),
                Some(&requested.version_key),
            ),
        ];
        fields
            .into_iter()
            .filter(|(_, recorded, requested)| recorded != requested)
            .map(|(field, recorded, requested)| FieldMismatch {
                field,
                recorded: recorded.map(str::to_string),
                requested: requested.map(str::to_string),
            })
            .collect()
    }
}

/// What `Db::enqueue` does when a job with the same [`ReviewIdentity`] is
/// already pending, in flight, or completed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnqueueMode {
    /// Return the covering job instead of enqueueing another one.
    Deduplicate,
    /// Explicit re-review: always enqueue a job in a new review round.
    NewRound,
}

impl EnqueueMode {
    pub fn as_str(self) -> &'static str {
        match self {
            EnqueueMode::Deduplicate => "deduplicate",
            EnqueueMode::NewRound => "new_round",
        }
    }
}

#[derive(Debug, Clone)]
pub struct EnqueueRequest {
    pub job: NewJob,
    /// Caller-chosen idempotency key, scoped to `job.project_id`. The first
    /// request with a key binds it to the job it resolved to; replaying the key
    /// returns that job for as long as it exists, even once it has finished.
    /// A new review round therefore needs a new key. `None` skips request
    /// idempotency and relies on [`EnqueueMode`] alone.
    pub request_key: Option<String>,
    pub mode: EnqueueMode,
    /// Which entry point asked, recorded on the enqueue event.
    pub source: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExistingReason {
    /// The request key was already bound to this job.
    RequestReplay,
    /// An active or completed job with the same review identity covers the request.
    Covered,
}

#[derive(Debug, Clone)]
pub enum EnqueueOutcome {
    Created(Job),
    Existing { job: Job, reason: ExistingReason },
}

impl EnqueueOutcome {
    pub fn job(&self) -> &Job {
        match self {
            EnqueueOutcome::Created(job) | EnqueueOutcome::Existing { job, .. } => job,
        }
    }

    pub fn into_job(self) -> Job {
        match self {
            EnqueueOutcome::Created(job) | EnqueueOutcome::Existing { job, .. } => job,
        }
    }

    pub fn is_created(&self) -> bool {
        matches!(self, EnqueueOutcome::Created(_))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FieldMismatch {
    pub field: &'static str,
    pub recorded: Option<String>,
    pub requested: Option<String>,
}

/// A request key was replayed with different review content. Nothing was
/// enqueued; the caller either resends the original request or picks a new key.
#[derive(Debug, Clone, thiserror::Error)]
#[error(
    "request key {request_key:?} in project {project_id} is already bound to job {existing_job_id} \
     with different content ({}); resend the original request to get that job back, \
     or use a new request key to ask for a different review",
    describe_mismatches(.mismatches)
)]
pub struct EnqueueConflict {
    pub project_id: String,
    pub request_key: String,
    pub existing_job_id: String,
    pub mismatches: Vec<FieldMismatch>,
}

fn describe_mismatches(mismatches: &[FieldMismatch]) -> String {
    let show = |value: &Option<String>| value.as_deref().unwrap_or("<unset>").to_string();
    mismatches
        .iter()
        .map(|m| {
            format!(
                "{}: recorded {}, requested {}",
                m.field,
                show(&m.recorded),
                show(&m.requested)
            )
        })
        .collect::<Vec<_>>()
        .join("; ")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusView {
    pub id: String,
    pub project_id: String,
    pub paper_id: String,
    pub backend: String,
    pub status: String,
    pub token: Option<String>,
    pub attempt: u32,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub next_poll_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub last_error: Option<String>,
    pub pdf_hash: String,
    pub git_tag: Option<String>,
    pub git_commit: Option<String>,
    pub version_no: u32,
    pub round_no: u32,
    pub version_source: String,
    pub version_key: String,
    pub score: Option<String>,
    pub summary_md: Option<String>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventRecord {
    pub id: i64,
    pub project_id: String,
    pub job_id: Option<String>,
    pub event_type: String,
    pub payload: Value,
    pub created_at: DateTime<Utc>,
}

/// A stored review, read through `Db::get_review`. `token` is the provider
/// token the review was fetched with, kept so callers can redact it; it is
/// not necessarily the job's current token.
#[derive(Debug, Clone)]
pub struct ReviewRecord {
    pub token: String,
    pub raw_json: Value,
    pub completed_at: DateTime<Utc>,
}

/// One row of the project registry (`projects` table): where the CLI last
/// found a project's `reviewloop.toml`.
#[derive(Debug, Clone)]
pub struct RegisteredProject {
    pub project_id: String,
    pub config_path: std::path::PathBuf,
    pub last_seen_at: DateTime<Utc>,
}
