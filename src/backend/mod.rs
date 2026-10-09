pub mod cspaper;
pub mod input;
pub mod stanford;

use crate::config::Config;
use crate::db::Db;
use crate::http::Redirects;
use crate::model::ReviewOptions;
use anyhow::Result;
use async_trait::async_trait;
use serde::Serialize;
use serde_json::Value;
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};
use thiserror::Error;

/// What the worker hands a backend. Everything that shapes the review comes
/// from the job row, so the request matches the job's recorded identity.
#[derive(Debug, Clone)]
pub struct SubmitRequest {
    pub pdf_path: PathBuf,
    pub email: String,
    pub venue: Option<String>,
    pub review_options: ReviewOptions,
    /// Where the backend reports each step it starts; see [`SubmitProgress`].
    pub progress: SubmitProgress,
}

/// One step of a provider submission, in the order they run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SubmitStep {
    /// Ask the provider for an upload target. Nothing exists remotely yet.
    UploadInit,
    /// Upload the PDF to that target. Still no submission.
    Upload,
    /// Ask the provider to accept the uploaded PDF: the step that creates the submission.
    Confirm,
}

impl SubmitStep {
    pub fn as_str(self) -> &'static str {
        match self {
            SubmitStep::UploadInit => "upload_init",
            SubmitStep::Upload => "upload",
            SubmitStep::Confirm => "confirm",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "upload_init" => Some(SubmitStep::UploadInit),
            "upload" => Some(SubmitStep::Upload),
            "confirm" => Some(SubmitStep::Confirm),
            _ => None,
        }
    }
}

/// The step a submission has reached, shared by the worker and the backend running it.
/// After the call, [`SubmitProgress::current`] names the step a failure happened in;
/// every step before it completed.
#[derive(Debug, Clone, Default)]
pub struct SubmitProgress(Arc<Mutex<Option<SubmitStep>>>);

impl SubmitProgress {
    pub fn enter(&self, step: SubmitStep) {
        *self.0.lock().expect("submit progress poisoned") = Some(step);
    }

    pub fn current(&self) -> Option<SubmitStep> {
        *self.0.lock().expect("submit progress poisoned")
    }
}

#[derive(Debug, Clone)]
pub struct SubmitReceipt {
    /// The provider's handle for the submission, persisted as the job's token
    /// and passed back to [`ReviewBackend::fetch_review`].
    pub token: String,
}

#[derive(Debug, Clone)]
pub enum ReviewFetchResult {
    Processing,
    Ready {
        raw_json: Value,
    },
    /// The provider finished the job without a review. Polling again gives
    /// the same answer; only a new submission can produce a review.
    Failed {
        reason: String,
    },
    InvalidToken,
}

#[derive(Debug, Error)]
pub enum BackendError {
    #[error("rate limited: {message}")]
    RateLimited {
        message: String,
        /// `Some(_)` when the server sent a parseable `Retry-After` header.
        /// `None` means honor the local polling cadence instead.
        retry_after: Option<chrono::Duration>,
    },
    #[error("server error ({status}): {body}")]
    Server { status: u16, body: String },
    #[error("schema error: {0}")]
    Schema(String),
    #[error("network error: {0}")]
    Network(String),
    #[error("command error: {0}")]
    Command(String),
    /// The provider refused the credentials, or none are configured. Nothing
    /// was created.
    #[error("authentication failed: {0}")]
    Auth(String),
    /// The provider definitively refused the request as invalid (unknown
    /// template, unusable file, missing field), or it was refused before
    /// sending. Nothing was created, so a corrected request may be resent.
    #[error("request rejected: {0}")]
    Rejected(String),
    /// A submit request may have been accepted by the provider, but no usable receipt
    /// came back. Never retried or redirected to the fallback automatically: the
    /// provider gives no idempotency guarantee, so a resend could duplicate the review.
    #[error("outcome unknown: {0}")]
    OutcomeUnknown(String),
}

#[async_trait]
pub trait ReviewBackend: Send + Sync {
    fn name(&self) -> &'static str;
    async fn submit(&self, req: SubmitRequest) -> std::result::Result<SubmitReceipt, BackendError>;
    async fn fetch_review(
        &self,
        token: &str,
    ) -> std::result::Result<ReviewFetchResult, BackendError>;
}

/// Which service produced a review, recorded with its artifacts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProviderSource {
    pub backend: String,
    pub name: String,
    pub base_url: Option<String>,
}

pub fn provider_source(config: &Config, backend: &str) -> ProviderSource {
    match backend {
        "stanford" => ProviderSource {
            backend: backend.to_string(),
            name: stanford::PROVIDER_NAME.to_string(),
            base_url: Some(config.providers.stanford.base_url.clone()),
        },
        cspaper::BACKEND => ProviderSource {
            backend: backend.to_string(),
            name: cspaper::PROVIDER_NAME.to_string(),
            base_url: Some(config.providers.cspaper.base_url.clone()),
        },
        other => ProviderSource {
            backend: other.to_string(),
            name: other.to_string(),
            base_url: None,
        },
    }
}

pub fn build_backend(
    config: &Config,
    backend: &str,
    db: Option<&Db>,
    project_id: Option<&str>,
) -> Result<Box<dyn ReviewBackend>> {
    match backend {
        "stanford" => {
            let client = crate::http::build_client(config, db, project_id, Redirects::Follow)?;
            Ok(Box::new(stanford::StanfordBackend::new(
                config.providers.stanford.base_url.clone(),
                client,
            )))
        }
        cspaper::BACKEND => {
            // The API key travels in a custom header that reqwest would forward
            // to a cross-host redirect target.
            let client = crate::http::build_client(config, db, project_id, Redirects::Refuse)?;
            Ok(Box::new(cspaper::CspaperBackend::new(
                &config.providers.cspaper,
                client,
            )))
        }
        other => anyhow::bail!("unsupported backend: {other}"),
    }
}

/// Whether a submission's token can still arrive by email after the submit
/// response was lost, so email ingestion may settle an uncertain submission.
/// CSPaper returns its job id only in the submit response and sends no email.
pub fn tokens_arrive_by_email(backend: &str) -> bool {
    backend != cspaper::BACKEND
}

/// Maximum retry delay we will ever honor from a server-supplied `Retry-After`
/// header. Caps malicious or buggy "5 years from now" dates so a single 429
/// cannot freeze a job indefinitely.
const MAX_RETRY_AFTER: chrono::Duration = chrono::Duration::seconds(24 * 60 * 60);

pub(crate) fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<chrono::Duration> {
    let raw = headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim();
    let parsed = if let Ok(secs) = raw.parse::<i64>() {
        // Clamp negative values to 0 so a server returning `-1` doesn't
        // produce a negative duration that confuses scheduling math.
        chrono::Duration::try_seconds(secs.max(0))?
    } else if let Ok(when) = chrono::DateTime::parse_from_rfc2822(raw) {
        let delta = when.with_timezone(&chrono::Utc) - chrono::Utc::now();
        if delta <= chrono::Duration::zero() {
            return Some(chrono::Duration::zero());
        }
        delta
    } else {
        return None;
    };
    // Cap at MAX_RETRY_AFTER to prevent a far-future date or huge integer
    // from freezing the job for an unbounded amount of time.
    Some(parsed.min(MAX_RETRY_AFTER))
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::{HeaderMap, HeaderValue, RETRY_AFTER};

    fn map_with(value: &str) -> HeaderMap {
        let mut m = HeaderMap::new();
        m.insert(RETRY_AFTER, HeaderValue::from_str(value).unwrap());
        m
    }

    #[test]
    fn retry_after_normal_seconds() {
        let d = parse_retry_after(&map_with("30")).unwrap();
        assert_eq!(d, chrono::Duration::seconds(30));
    }

    #[test]
    fn retry_after_huge_integer_capped_to_24h() {
        let d = parse_retry_after(&map_with("99999999")).unwrap();
        assert_eq!(d, MAX_RETRY_AFTER);
    }

    #[test]
    fn retry_after_negative_clamped_to_zero() {
        let d = parse_retry_after(&map_with("-5")).unwrap();
        assert_eq!(d, chrono::Duration::zero());
    }

    #[test]
    fn retry_after_past_rfc2822_date_returns_zero() {
        // 5 years ago — delta is negative, must clamp to zero
        let past = chrono::Utc::now() - chrono::Duration::days(5 * 365);
        let rfc = past.format("%a, %d %b %Y %H:%M:%S GMT").to_string();
        let d = parse_retry_after(&map_with(&rfc)).unwrap();
        assert_eq!(d, chrono::Duration::zero());
    }

    #[test]
    fn retry_after_far_future_rfc2822_capped_to_24h() {
        // 1 year in the future — must be capped to 24 h
        let future = chrono::Utc::now() + chrono::Duration::days(365);
        let rfc = future.format("%a, %d %b %Y %H:%M:%S GMT").to_string();
        let d = parse_retry_after(&map_with(&rfc)).unwrap();
        assert_eq!(d, MAX_RETRY_AFTER);
    }

    #[test]
    fn retry_after_missing_header_returns_none() {
        assert!(parse_retry_after(&HeaderMap::new()).is_none());
    }

    #[test]
    fn retry_after_garbage_returns_none() {
        assert!(parse_retry_after(&map_with("not-a-date-or-number")).is_none());
    }
}
