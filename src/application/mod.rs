//! Review operations shared by the CLI and the MCP adapter.
//!
//! Every operation is synchronous and works only on a loaded [`Config`], the
//! [`Db`] and the state directory (review artifacts and PDF snapshots).
//! Operations never print (state changes emit `tracing` events at INFO), never
//! exit the process and never contact a review provider: submitting to and
//! polling the provider stay with the caller (the CLI's immediate submit, the daemon's
//! worker). Results are token-free DTOs that serialize to the JSON documented
//! in `docs/review-operations.md`; failures are [`OpError`]s with a stable
//! [`OpError::code`].
//!
//! The project a [`ReviewOps`] acts on is the `project_id` of its config. An
//! empty `project_id` (no `reviewloop.toml`, as for the menu bar) leaves job-id
//! lookups unscoped; operations that need a project then fail with
//! [`OpError::ProjectRequired`].
//!
//! [`Config`]: crate::config::Config
//! [`Db`]: crate::db::Db

mod dto;
mod error;
mod operation;
mod ops;
mod redact;
mod request;

pub use dto::{
    JobCandidate, JobList, JobPhase, JobView, ManuscriptInput, PaperView, ProjectView,
    RequestDisposition, RetryAction, RetryOutcome, ReviewArtifacts, ReviewRequestOutcome,
    ReviewSection, ReviewView, TransitionOutcome,
};
pub use error::{ErrorView, OpError};
pub use operation::Operation;
pub use ops::{
    CANCELLED_BY_USER, DEFAULT_JOB_LIST_LIMIT, MAX_JOB_LIST_LIMIT, ReviewOps, require_project,
};
pub use redact::{redact_text, redact_value};
pub use request::{
    Approval, CancelRequest, Eligibility, JobListQuery, JobRef, RequestOrigin, RetryRequest,
    ReviewPart, ReviewQuery, ReviewRequest,
};
