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
