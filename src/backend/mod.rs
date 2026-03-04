pub mod stanford;

use crate::config::Config;
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct SubmitRequest {
    pub pdf_path: PathBuf,
    pub email: String,
    pub venue: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SubmitReceipt {
    pub token: String,
    pub backend_submission_ref: Option<String>,
}

#[derive(Debug, Clone)]
pub enum ReviewFetchResult {
    Processing,
    Ready { raw_json: Value },
    InvalidToken,
}

#[derive(Debug, Error)]
pub enum BackendError {
    #[error("rate limited: {0}")]
    RateLimited(String),
    #[error("server error ({status}): {body}")]
    Server { status: u16, body: String },
    #[error("schema error: {0}")]
    Schema(String),
    #[error("network error: {0}")]
    Network(String),
    #[error("command error: {0}")]
    Command(String),
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

pub fn build_backend(config: &Config, backend: &str) -> Result<Box<dyn ReviewBackend>> {
    match backend {
        "stanford" => Ok(Box::new(stanford::StanfordBackend::new(
            config.providers.stanford.base_url.clone(),
        ))),
        other => anyhow::bail!("unsupported backend: {other}"),
    }
}
