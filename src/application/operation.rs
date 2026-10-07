/// The public review operations. The mapping to MCP tool names is part of the
/// contract in `docs/review-operations.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Operation {
    ListProjects,
    ListPapers,
    RequestReview,
    GetJob,
    ListJobs,
    GetReview,
    ApproveJob,
    RetryJob,
    CancelJob,
}

impl Operation {
    pub const ALL: [Operation; 9] = [
        Operation::ListProjects,
        Operation::ListPapers,
        Operation::RequestReview,
        Operation::GetJob,
        Operation::ListJobs,
        Operation::GetReview,
        Operation::ApproveJob,
        Operation::RetryJob,
        Operation::CancelJob,
    ];

    /// The MCP tool name, which is also the name of the
    /// [`ReviewOps`](super::ReviewOps) method implementing the operation.
    pub fn tool_name(self) -> &'static str {
        match self {
            Operation::ListProjects => "list_projects",
            Operation::ListPapers => "list_papers",
            Operation::RequestReview => "request_review",
            Operation::GetJob => "get_job",
            Operation::ListJobs => "list_jobs",
            Operation::GetReview => "get_review",
            Operation::ApproveJob => "approve_job",
            Operation::RetryJob => "retry_job",
            Operation::CancelJob => "cancel_job",
        }
    }

    /// Read-only operations never write the database or the filesystem.
    pub fn read_only(self) -> bool {
        matches!(
            self,
            Operation::ListProjects
                | Operation::ListPapers
                | Operation::GetJob
                | Operation::ListJobs
                | Operation::GetReview
        )
    }
}
