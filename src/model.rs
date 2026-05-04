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
    pub pdf_path: String,
    pub pdf_hash: String,
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
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewJob {
    pub project_id: String,
    pub paper_id: String,
    pub backend: String,
    pub pdf_path: String,
    pub pdf_hash: String,
    pub status: JobStatus,
    pub email: String,
    pub venue: Option<String>,
    pub git_tag: Option<String>,
    pub git_commit: Option<String>,
    pub next_poll_at: Option<DateTime<Utc>>,
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
