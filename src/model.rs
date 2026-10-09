use crate::submission_input::PreparedInput;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

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
    use super::{Job, JobStatus, ReviewIdentity, ReviewOptions};
    use chrono::Utc;

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

    const DESK_ON: &str = r#"{"desk_rejection_enabled":"true"}"#;
    const DESK_OFF: &str = r#"{"desk_rejection_enabled":"false"}"#;

    fn desk(enabled: bool) -> ReviewOptions {
        ReviewOptions::default().with("desk_rejection_enabled", enabled.to_string())
    }

    fn identity(
        backend: &str,
        venue: Option<&str>,
        options: &ReviewOptions,
        git_commit: Option<&str>,
    ) -> ReviewIdentity {
        ReviewIdentity::new("paper-a", backend, "hash-a", venue, options, git_commit)
    }

    #[test]
    fn review_options_canonical_sorts_keys_and_is_none_when_empty() {
        assert_eq!(ReviewOptions::default().canonical(), None);
        let options = ReviewOptions::default()
            .with("zeta", "2")
            .with("alpha", "1");
        assert_eq!(
            options.canonical().as_deref(),
            Some(r#"{"alpha":"1","zeta":"2"}"#)
        );
        // Insertion order never changes the stored form.
        let reordered = ReviewOptions::default()
            .with("alpha", "1")
            .with("zeta", "2");
        assert_eq!(reordered.canonical(), options.canonical());
        assert_eq!(desk(true).canonical().as_deref(), Some(DESK_ON));
    }

    #[test]
    fn review_options_from_canonical_round_trips() {
        assert_eq!(
            ReviewOptions::from_canonical(None).expect("NULL column"),
            ReviewOptions::default()
        );
        assert_eq!(
            ReviewOptions::from_canonical(Some("")).expect("empty column"),
            ReviewOptions::default()
        );
        assert_eq!(
            ReviewOptions::from_canonical(Some("  ")).expect("blank column"),
            ReviewOptions::default()
        );
        for options in [desk(true), desk(false), desk(true).with("other", "x")] {
            let canonical = options.canonical().expect("non-empty options");
            assert_eq!(
                ReviewOptions::from_canonical(Some(&canonical)).expect("canonical JSON"),
                options
            );
        }
        assert!(ReviewOptions::from_canonical(Some("not json")).is_err());
    }

    #[test]
    fn identity_without_options_serializes_like_pre_oss_353_identity() {
        let stanford = identity("stanford", Some(" ICLR "), &ReviewOptions::default(), None);
        assert_eq!(
            serde_json::to_string(&stanford).expect("serialize"),
            r#"{"paper_id":"paper-a","backend":"stanford","pdf_hash":"hash-a","venue":"ICLR","version_source":"pdf_hash","version_key":"hash-a"}"#
        );

        let cspaper = identity("cspaper", Some("T"), &desk(false), Some("abc123"));
        assert_eq!(
            serde_json::to_string(&cspaper).expect("serialize"),
            r#"{"paper_id":"paper-a","backend":"cspaper","pdf_hash":"hash-a","venue":"T","review_options":{"desk_rejection_enabled":"false"},"version_source":"git_commit","version_key":"abc123"}"#
        );
    }

    #[test]
    fn identity_json_without_options_reads_back_with_empty_options() {
        let legacy = r#"{"paper_id":"paper-a","backend":"stanford","pdf_hash":"hash-a","venue":"ICLR","version_source":"pdf_hash","version_key":"hash-a"}"#;
        let read: ReviewIdentity = serde_json::from_str(legacy).expect("legacy identity_json");
        assert!(read.review_options.is_empty());
        let current = identity("stanford", Some("ICLR"), &ReviewOptions::default(), None);
        assert_eq!(read, current);
        assert!(read.mismatches(&current).is_empty());

        let with_options = identity("cspaper", Some("T"), &desk(true), None);
        let json = serde_json::to_string(&with_options).expect("serialize");
        let back: ReviewIdentity = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, with_options);
    }

    #[test]
    fn mismatches_report_review_options_after_venue_as_canonical_text() {
        let recorded = identity("cspaper", Some("T1"), &desk(true), Some("abc123"));
        let requested = identity("cspaper", Some("T2"), &desk(false), None);
        let mismatches = recorded.mismatches(&requested);
        let fields: Vec<&str> = mismatches.iter().map(|m| m.field).collect();
        assert_eq!(
            fields,
            ["venue", "review_options", "version_source", "version_key"]
        );
        assert_eq!(mismatches[1].recorded.as_deref(), Some(DESK_ON));
        assert_eq!(mismatches[1].requested.as_deref(), Some(DESK_OFF));

        // Options recorded before they existed compare as unset.
        let legacy = identity("cspaper", Some("T1"), &ReviewOptions::default(), None);
        let current = identity("cspaper", Some("T1"), &desk(true), None);
        let mismatches = legacy.mismatches(&current);
        assert_eq!(mismatches.len(), 1, "{mismatches:?}");
        assert_eq!(mismatches[0].field, "review_options");
        assert_eq!(mismatches[0].recorded, None);
        assert_eq!(mismatches[0].requested.as_deref(), Some(DESK_ON));

        assert!(current.mismatches(&current.clone()).is_empty());
    }

    fn tokenless_job(backend: &str) -> Job {
        let now = Utc::now();
        Job {
            id: "job-1".to_string(),
            project_id: "project".to_string(),
            paper_id: "paper-a".to_string(),
            backend: backend.to_string(),
            pdf_path: "paper.pdf".to_string(),
            pdf_hash: "hash-a".to_string(),
            snapshot_path: None,
            status: JobStatus::Submitted,
            token: None,
            email: String::new(),
            venue: None,
            review_options: ReviewOptions::default(),
            git_tag: None,
            git_commit: None,
            version_no: 1,
            round_no: 1,
            version_source: "pdf_hash".to_string(),
            version_key: "hash-a".to_string(),
            attempt: 1,
            started_at: None,
            next_poll_at: None,
            last_error: None,
            fallback_used: false,
            lease_owner: None,
            lease_expires_at: None,
            submit_stage: Some(super::SubmitStage::Uncertain),
            created_at: now,
            updated_at: now,
        }
    }

    #[test]
    fn reconcile_hint_for_tokenless_cspaper_job_never_waits_for_email() {
        let hint = tokenless_job("cspaper").reconcile_hint();
        for expected in [
            "CSPaper sends no email",
            "`reviewloop import-token --job-id job-1 --token <cspaper job_id>`",
            "`reviewloop retry --job-id job-1 --force`",
            "`reviewloop cancel --job-id job-1`",
        ] {
            assert!(hint.contains(expected), "missing {expected:?}: {hint}");
        }
        assert!(!hint.contains("review email"), "{hint}");

        // The email wording stays Stanford's.
        assert!(
            tokenless_job("stanford")
                .reconcile_hint()
                .contains("once the review email arrives")
        );

        // A saved receipt resumes polling the same way for every backend.
        let mut with_token = tokenless_job("cspaper");
        with_token.token = Some("job_abc".to_string());
        assert_eq!(
            with_token.reconcile_hint(),
            "a receipt token is saved; run `reviewloop retry --job-id job-1` to resume polling"
        );
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
    pub review_options: ReviewOptions,
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
        if self.backend == crate::backend::cspaper::BACKEND {
            return format!(
                "not resubmitted automatically; CSPaper sends no email, so look for this paper in the CSPaper review list (https://cspaper.org/platform/review) and run `reviewloop import-token --job-id {} --token <cspaper job_id>`, or `reviewloop retry --job-id {} --force` to resubmit anyway (may duplicate), or `reviewloop cancel --job-id {}`",
                self.id, self.id, self.id
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
    pub review_options: ReviewOptions,
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
            &self.review_options,
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

/// Provider settings beyond the venue that change what a review means, such as
/// CSPaper's desk-rejection screening. Keys are the provider's option names.
/// Empty for providers without such settings, which keeps their identity and
/// stored rows unchanged.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ReviewOptions(BTreeMap<String, String>);

impl ReviewOptions {
    pub fn with(mut self, key: &str, value: impl Into<String>) -> Self {
        self.0.insert(key.to_string(), value.into());
        self
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Sorted-key JSON, or `None` when empty: the form stored in
    /// `jobs.review_options` and compared by coverage and request keys.
    pub fn canonical(&self) -> Option<String> {
        (!self.is_empty())
            .then(|| serde_json::to_string(&self.0).expect("a string map always serializes"))
    }

    pub fn from_canonical(raw: Option<&str>) -> serde_json::Result<Self> {
        match raw.map(str::trim).filter(|raw| !raw.is_empty()) {
            Some(raw) => serde_json::from_str(raw).map(Self),
            None => Ok(Self::default()),
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
    /// Trimmed; `None` when unset or blank. For CSPaper this is the review
    /// template (`agent_id`).
    pub venue: Option<String>,
    /// Omitted when empty, so identities recorded before options existed
    /// still read back and compare equal.
    #[serde(default, skip_serializing_if = "ReviewOptions::is_empty")]
    pub review_options: ReviewOptions,
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
        review_options: &ReviewOptions,
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
            review_options: review_options.clone(),
            version_source,
            version_key,
        }
    }

    /// Fields whose values differ between `self` (the recorded request) and
    /// `requested`, in declaration order.
    pub fn mismatches(&self, requested: &Self) -> Vec<FieldMismatch> {
        let recorded_options = self.review_options.canonical();
        let requested_options = requested.review_options.canonical();
        let fields: [(&'static str, Option<&str>, Option<&str>); 7] = [
            ("paper_id", Some(&self.paper_id), Some(&requested.paper_id)),
            ("backend", Some(&self.backend), Some(&requested.backend)),
            ("pdf_hash", Some(&self.pdf_hash), Some(&requested.pdf_hash)),
            ("venue", self.venue.as_deref(), requested.venue.as_deref()),
            (
                "review_options",
                recorded_options.as_deref(),
                requested_options.as_deref(),
            ),
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
