use super::{dto::JobCandidate, operation::Operation};
use crate::{
    config::Config,
    model::{EnqueueConflict, JobStatus},
};
use serde::Serialize;
use serde_json::{Value, json};

/// A failed review operation. [`OpError::code`] is stable and documented in
/// `docs/review-operations.md`; the `Display` text is the message the CLI has
/// always printed and may change.
#[derive(Debug, thiserror::Error)]
pub enum OpError {
    #[error(
        "this command requires a project config. run `reviewloop init project --project-id <id>` in your repo first"
    )]
    ProjectRequired,

    #[error("{}", project_mismatch_message(.job_id, .job_project_id, .context_project_id))]
    ProjectMismatch {
        job_id: String,
        job_project_id: String,
        context_project_id: String,
    },

    #[error("{}", paper_not_found_message(.paper_id, .known))]
    PaperNotFound {
        paper_id: String,
        known: Vec<String>,
    },

    #[error("{message}")]
    InvalidRequest {
        field: &'static str,
        message: String,
    },

    /// A request key was replayed with different review content.
    #[error(transparent)]
    RequestConflict(EnqueueConflict),

    #[error("pdf file not found: {path}")]
    PdfNotFound { paper_id: String, path: String },

    /// `detail` is the resolver's full error chain.
    #[error("{detail}")]
    SubmitterEmailUnavailable { backend: String, detail: String },

    #[error("job not found: {job_id}")]
    JobNotFound { job_id: String },

    #[error(
        "no {action}-eligible job for paper_id={paper_id} (looking for statuses: {})",
        status_list(.statuses)
    )]
    NoEligibleJob {
        paper_id: String,
        action: &'static str,
        statuses: &'static [JobStatus],
    },

    #[error(
        "multiple jobs match paper_id={paper_id} for {action}; pass --job-id explicitly. candidates: {}",
        candidate_list(.candidates)
    )]
    AmbiguousJob {
        paper_id: String,
        action: &'static str,
        candidates: Vec<JobCandidate>,
    },

    /// The job's status does not allow `operation`. `message` is the CLI text.
    #[error("{message}")]
    InvalidState {
        job_id: String,
        status: JobStatus,
        operation: Operation,
        message: String,
    },

    #[error("review not available for job {job_id}: status is {}", .status.as_str())]
    ReviewNotAvailable { job_id: String, status: JobStatus },

    #[error(
        "review for job {job_id} has no section {section:?}; available: {}",
        .available.join(", ")
    )]
    SectionNotFound {
        job_id: String,
        section: String,
        available: Vec<String>,
    },

    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

fn project_mismatch_message(job_id: &str, job_project: &str, context_project: &str) -> String {
    if context_project.is_empty() {
        format!(
            "job {job_id} belongs to project {job_project}; retry it with that project's config"
        )
    } else {
        format!(
            "job {job_id} belongs to project {job_project}, but the loaded config declares project {context_project}"
        )
    }
}

fn status_list(statuses: &[JobStatus]) -> String {
    statuses
        .iter()
        .map(|status| status.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

/// The CLI shows at most five candidates; the error keeps all of them.
fn candidate_list(candidates: &[JobCandidate]) -> String {
    candidates
        .iter()
        .take(5)
        .map(|candidate| format!("{} ({})", candidate.job_id, candidate.status.as_str()))
        .collect::<Vec<_>>()
        .join(", ")
}

fn paper_not_found_message(paper_id: &str, known: &[String]) -> String {
    if known.is_empty() {
        format!(
            "paper_id not found: {paper_id}\n  \
             no papers configured yet — add one with `reviewloop paper add --paper-id {paper_id} --pdf-path <path>`"
        )
    } else {
        format!(
            "paper_id not found: {paper_id}\n  \
             known paper_ids: {}\n  \
             add this paper with `reviewloop paper add --paper-id {paper_id} --pdf-path <path>`",
            known.join(", ")
        )
    }
}

impl OpError {
    /// A missing paper, listing the papers `config` does know.
    pub fn paper_not_found(paper_id: &str, config: &Config) -> Self {
        OpError::PaperNotFound {
            paper_id: paper_id.to_string(),
            known: config.papers.iter().map(|paper| paper.id.clone()).collect(),
        }
    }

    pub fn code(&self) -> &'static str {
        match self {
            OpError::ProjectRequired => "project_required",
            OpError::ProjectMismatch { .. } => "project_mismatch",
            OpError::InvalidRequest { .. } => "invalid_request",
            OpError::RequestConflict(_) => "request_conflict",
            OpError::PaperNotFound { .. } => "paper_not_found",
            OpError::PdfNotFound { .. } => "pdf_not_found",
            OpError::SubmitterEmailUnavailable { .. } => "submitter_email_unavailable",
            OpError::JobNotFound { .. } => "job_not_found",
            OpError::NoEligibleJob { .. } => "no_eligible_job",
            OpError::AmbiguousJob { .. } => "ambiguous_job",
            OpError::InvalidState { .. } => "invalid_state",
            OpError::ReviewNotAvailable { .. } => "review_not_available",
            OpError::SectionNotFound { .. } => "section_not_found",
            OpError::Internal(_) => "internal",
        }
    }

    /// What the caller can do about the failure, when there is something.
    pub fn recovery(&self) -> Option<String> {
        let hint = match self {
            OpError::ProjectRequired => {
                "run `reviewloop init project --project-id <id>` in the paper repository, or call the operation for a registered project (list_projects)".to_string()
            }
            OpError::ProjectMismatch {
                job_project_id, ..
            } => format!(
                "call the operation with project {job_project_id}'s configuration (run it from that repository)"
            ),
            OpError::InvalidRequest { field, .. } => format!("fix {field} and send the request again"),
            OpError::RequestConflict(conflict) => format!(
                "the manuscript or its settings changed since this key was used; read job {} with get_job, or send a new request_key to review the current version",
                conflict.existing_job_id
            ),
            OpError::PaperNotFound { paper_id, .. } => format!(
                "use a paper_id from list_papers, or register the paper with `reviewloop paper add --paper-id {paper_id} --pdf-path <path>`"
            ),
            OpError::PdfNotFound { path, .. } => format!(
                "restore the PDF at {path} or point the paper's pdf_path in reviewloop.toml at the current file"
            ),
            OpError::SubmitterEmailUnavailable { .. } => {
                "set providers.stanford.email in ~/.config/reviewloop/config.toml or run `reviewloop email login --provider google`".to_string()
            }
            OpError::JobNotFound { .. } => {
                "check the job_id; list_jobs shows this project's jobs".to_string()
            }
            OpError::NoEligibleJob { .. } => {
                "list the paper's jobs with list_jobs and pass a job_id".to_string()
            }
            OpError::AmbiguousJob { .. } => {
                "pass one of the candidate job ids explicitly".to_string()
            }
            OpError::InvalidState {
                operation, status, ..
            } => match operation {
                Operation::ApproveJob => "only PENDING_APPROVAL jobs need approval; check the job with get_job".to_string(),
                Operation::CancelJob => "the job has already finished; use retry_job to run it again".to_string(),
                Operation::RetryJob if *status == JobStatus::PendingApproval => {
                    "approve the job with approve_job".to_string()
                }
                Operation::RetryJob if *status == JobStatus::Submitted => {
                    "the submission is in flight or already has a receipt: check submit_stage with get_job, then wait, retry_job again to poll a saved receipt, or cancel_job".to_string()
                }
                Operation::RetryJob => "retry without force, or check the job's status with get_job".to_string(),
                _ => "check the job's status with get_job".to_string(),
            },
            OpError::ReviewNotAvailable { status, .. } if status.is_terminal() => {
                "the job ended without a review (see last_error); re-run it with retry_job or request_review".to_string()
            }
            OpError::ReviewNotAvailable { .. } => {
                "wait until the job is COMPLETED; check it again with get_job after next_poll_at".to_string()
            }
            OpError::SectionNotFound { .. } => {
                "request one of the available sections, or the markdown part".to_string()
            }
            OpError::Internal(_) => return None,
        };
        Some(hint)
    }

    /// The variant's fields, for callers that act on them (for example
    /// picking one of `candidates`).
    pub fn details(&self) -> Value {
        match self {
            OpError::ProjectRequired | OpError::Internal(_) => json!({}),
            OpError::ProjectMismatch {
                job_id,
                job_project_id,
                context_project_id,
            } => json!({
                "job_id": job_id,
                "job_project_id": job_project_id,
                "context_project_id": context_project_id,
            }),
            OpError::InvalidRequest { field, .. } => json!({ "field": field }),
            OpError::RequestConflict(conflict) => json!({
                "project_id": conflict.project_id,
                "request_key": conflict.request_key,
                "existing_job_id": conflict.existing_job_id,
                "mismatches": conflict.mismatches,
            }),
            OpError::PaperNotFound { paper_id, known } => {
                json!({ "paper_id": paper_id, "known": known })
            }
            OpError::PdfNotFound { paper_id, path } => {
                json!({ "paper_id": paper_id, "path": path })
            }
            OpError::SubmitterEmailUnavailable { backend, .. } => json!({ "backend": backend }),
            OpError::JobNotFound { job_id } => json!({ "job_id": job_id }),
            OpError::NoEligibleJob {
                paper_id,
                action,
                statuses,
            } => json!({
                "paper_id": paper_id,
                "action": action,
                "statuses": statuses.iter().map(|status| status.as_str()).collect::<Vec<_>>(),
            }),
            OpError::AmbiguousJob {
                paper_id,
                action,
                candidates,
            } => json!({ "paper_id": paper_id, "action": action, "candidates": candidates }),
            OpError::InvalidState {
                job_id,
                status,
                operation,
                ..
            } => json!({
                "job_id": job_id,
                "status": status.as_str(),
                "operation": operation.tool_name(),
            }),
            OpError::ReviewNotAvailable { job_id, status } => {
                json!({ "job_id": job_id, "status": status.as_str() })
            }
            OpError::SectionNotFound {
                job_id,
                section,
                available,
            } => json!({ "job_id": job_id, "section": section, "available": available }),
        }
    }

    pub fn view(&self) -> ErrorView {
        ErrorView {
            code: self.code(),
            message: format!("{self:#}"),
            recovery: self.recovery(),
            details: self.details(),
        }
    }
}

/// The serializable form of an [`OpError`].
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ErrorView {
    pub code: &'static str,
    pub message: String,
    pub recovery: Option<String>,
    /// The variant's fields; `{}` when it has none.
    pub details: Value,
}
