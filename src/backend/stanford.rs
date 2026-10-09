//! Stanford Agentic Reviewer (`paperreview.ai`). The contract this adapter follows,
//! with the evidence for each part, is in `docs/providers/stanford.md`.

use super::{
    BackendError, ReviewBackend, ReviewFetchResult, SubmitReceipt, SubmitRequest, SubmitStep,
    input::{InputPolicy, upload_file_name},
    parse_retry_after,
};
use crate::http::describe_error;
use async_trait::async_trait;
use reqwest::{StatusCode, multipart};
use reqwest_middleware::ClientWithMiddleware;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::HashMap, path::Path, time::Duration as StdDuration};

pub const PROVIDER_NAME: &str = "Stanford Agentic Reviewer";

/// The upload form's published limits: "Max 10MB • First 15 pages analyzed"; its
/// script rejects files over `10 * 1024 * 1024` bytes.
pub const INPUT_POLICY: InputPolicy = InputPolicy {
    max_bytes: 10 * 1024 * 1024,
    reviewed_pages: Some(15),
};

/// Bounds on each provider request. Every submit step together stays well inside the
/// worker's 20-minute dispatch bound, so a stuck step is reported as that step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StepTimeouts {
    /// `get-upload-url`: no submission exists yet, so a timeout is definitive.
    pub upload_init: StdDuration,
    /// The PDF upload to the presigned target (up to 10 MiB); still definitive.
    pub upload: StdDuration,
    /// `confirm-upload`: a timeout leaves the outcome unknown.
    pub confirm: StdDuration,
    /// One review poll.
    pub fetch: StdDuration,
}

impl Default for StepTimeouts {
    fn default() -> Self {
        Self {
            upload_init: StdDuration::from_secs(30),
            upload: StdDuration::from_secs(5 * 60),
            confirm: StdDuration::from_secs(5 * 60),
            fetch: StdDuration::from_secs(2 * 60),
        }
    }
}

/// The provider's error text: FastAPI's `detail`, either a message or a list of
/// validation errors, else the raw body.
fn provider_detail(body: &str) -> String {
    #[derive(Deserialize)]
    struct ErrorBody {
        detail: Value,
    }
    let Ok(ErrorBody { detail }) = serde_json::from_str::<ErrorBody>(body) else {
        return body.trim().to_string();
    };
    match detail {
        Value::String(message) => message,
        Value::Array(errors) => errors
            .iter()
            .map(|error| {
                let message = error
                    .get("msg")
                    .and_then(Value::as_str)
                    .unwrap_or("invalid value");
                let field = error
                    .get("loc")
                    .and_then(Value::as_array)
                    .map(|loc| {
                        loc.iter()
                            .filter(|part| part.as_str() != Some("body"))
                            .map(|part| match part {
                                Value::String(name) => name.clone(),
                                other => other.to_string(),
                            })
                            .collect::<Vec<_>>()
                            .join(".")
                    })
                    .unwrap_or_default();
                if field.is_empty() {
                    message.to_string()
                } else {
                    format!("{field}: {message}")
                }
            })
            .collect::<Vec<_>>()
            .join("; "),
        other => other.to_string(),
    }
}

/// Why a request got no response, without its URL.
fn send_failure(err: &reqwest_middleware::Error, limit: StdDuration) -> String {
    if err.is_timeout() {
        format!("timed out after {}s", limit.as_secs_f64())
    } else {
        format!("request failed: {}", describe_error(err, err.url()))
    }
}

/// A reply read to the end.
struct Reply {
    status: StatusCode,
    retry_after: Option<chrono::Duration>,
    body: Result<String, String>,
}

impl Reply {
    async fn read(resp: reqwest::Response) -> Self {
        let status = resp.status();
        let retry_after = parse_retry_after(resp.headers());
        let body = resp
            .text()
            .await
            .map_err(|err| describe_error(&err, err.url()));
        Self {
            status,
            retry_after,
            body,
        }
    }

    /// The error detail of a body that may not have been readable.
    fn detail(&self) -> String {
        match &self.body {
            Ok(body) => provider_detail(body),
            Err(err) => format!("unreadable body: {err}"),
        }
    }

    fn rate_limited(&self) -> BackendError {
        BackendError::RateLimited {
            message: self.detail(),
            retry_after: self.retry_after,
        }
    }
}

#[derive(Debug, Deserialize)]
struct UploadUrlResponse {
    success: bool,
    presigned_url: Option<String>,
    s3_key: Option<String>,
    presigned_fields: Option<HashMap<String, String>>,
    detail: Option<String>,
}

/// Where `get-upload-url` told us to upload.
struct UploadTarget {
    presigned_url: String,
    s3_key: String,
    presigned_fields: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct ConfirmResponse {
    success: bool,
    token: Option<String>,
    detail: Option<String>,
    message: Option<String>,
}

#[derive(Clone)]
pub struct StanfordBackend {
    client: ClientWithMiddleware,
    base_url: String,
    timeouts: StepTimeouts,
}

impl StanfordBackend {
    pub fn new(base_url: String, client: ClientWithMiddleware) -> Self {
        Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            timeouts: StepTimeouts::default(),
        }
    }

    pub fn with_timeouts(self, timeouts: StepTimeouts) -> Self {
        Self { timeouts, ..self }
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    async fn request_upload_target(
        &self,
        file_name: &str,
        venue: &str,
    ) -> Result<UploadTarget, BackendError> {
        let limit = self.timeouts.upload_init;
        let resp = self
            .client
            .post(self.endpoint("/api/get-upload-url"))
            .timeout(limit)
            .json(&json!({ "filename": file_name, "venue": venue }))
            .send()
            .await
            .map_err(|e| {
                BackendError::Network(format!("get-upload-url {}", send_failure(&e, limit)))
            })?;
        let reply = Reply::read(resp).await;
        if reply.status == StatusCode::TOO_MANY_REQUESTS {
            return Err(reply.rate_limited());
        }
        if reply.status.is_server_error() {
            return Err(BackendError::Server {
                status: reply.status.as_u16(),
                body: reply.detail(),
            });
        }
        if !reply.status.is_success() {
            return Err(BackendError::Schema(format!(
                "get-upload-url failed ({}): {}",
                reply.status,
                reply.detail()
            )));
        }
        let body = reply.body.map_err(|err| {
            BackendError::Network(format!("get-upload-url body unreadable: {err}"))
        })?;
        let parsed: UploadUrlResponse = serde_json::from_str(&body)
            .map_err(|e| BackendError::Schema(format!("invalid get-upload-url payload: {e}")))?;
        if !parsed.success {
            return Err(BackendError::Schema(parsed.detail.unwrap_or_else(|| {
                "get-upload-url returned success=false".to_string()
            })));
        }
        let missing =
            |field: &str| BackendError::Schema(format!("get-upload-url: missing {field}"));
        Ok(UploadTarget {
            presigned_url: parsed
                .presigned_url
                .ok_or_else(|| missing("presigned_url"))?,
            s3_key: parsed.s3_key.ok_or_else(|| missing("s3_key"))?,
            presigned_fields: parsed
                .presigned_fields
                .ok_or_else(|| missing("presigned_fields"))?,
        })
    }

    async fn upload(
        &self,
        target: UploadTarget,
        pdf_path: &Path,
        file_name: String,
    ) -> Result<String, BackendError> {
        let file_bytes = tokio::fs::read(pdf_path)
            .await
            .map_err(|e| BackendError::Network(format!("failed to read PDF: {e}")))?;
        // The presigned fields must precede the file in a presigned POST.
        let form = target
            .presigned_fields
            .into_iter()
            .fold(multipart::Form::new(), |form, (k, v)| form.text(k, v));
        let file_part = multipart::Part::bytes(file_bytes)
            .file_name(file_name)
            .mime_str("application/pdf")
            .map_err(|e| BackendError::Schema(format!("invalid mime: {e}")))?;

        let limit = self.timeouts.upload;
        let resp = self
            .client
            .post(target.presigned_url)
            .timeout(limit)
            .multipart(form.part("file", file_part))
            .send()
            .await
            .map_err(|e| BackendError::Network(format!("S3 upload {}", send_failure(&e, limit))))?;
        let reply = Reply::read(resp).await;
        if reply.status.is_success() {
            return Ok(target.s3_key);
        }
        let body = reply
            .body
            .unwrap_or_else(|err| format!("unreadable body: {err}"));
        if reply.status.is_server_error() {
            return Err(BackendError::Server {
                status: reply.status.as_u16(),
                body,
            });
        }
        Err(BackendError::Schema(format!(
            "S3 upload rejected ({}): {body}",
            reply.status
        )))
    }

    /// `confirm-upload` creates the submission. Everything before it is safe to retry;
    /// from here on, any failure that does not prove rejection is `OutcomeUnknown`, so
    /// the worker never resends blindly.
    async fn confirm(
        &self,
        s3_key: String,
        venue: String,
        email: String,
    ) -> Result<SubmitReceipt, BackendError> {
        let form = multipart::Form::new()
            .text("s3_key", s3_key)
            .text("venue", venue)
            .text("email", email);
        let limit = self.timeouts.confirm;
        let resp = self
            .client
            .post(self.endpoint("/api/confirm-upload"))
            .timeout(limit)
            .multipart(form)
            .send()
            .await
            .map_err(|e| {
                // A connection that never opened carried nothing.
                if e.is_connect() {
                    BackendError::Network(format!("confirm-upload {}", send_failure(&e, limit)))
                } else if e.is_timeout() {
                    BackendError::OutcomeUnknown(format!(
                        "confirm-upload got no response within {}s",
                        limit.as_secs_f64()
                    ))
                } else {
                    BackendError::OutcomeUnknown(format!(
                        "confirm-upload got no response: {}",
                        describe_error(&e, e.url())
                    ))
                }
            })?;

        let reply = Reply::read(resp).await;
        if reply.status == StatusCode::TOO_MANY_REQUESTS {
            return Err(reply.rate_limited());
        }
        if reply.status.is_server_error() {
            return Err(BackendError::OutcomeUnknown(format!(
                "confirm-upload returned {}: {}",
                reply.status,
                reply.detail()
            )));
        }
        if !reply.status.is_success() {
            return Err(BackendError::Schema(format!(
                "confirm-upload failed ({}): {}",
                reply.status,
                reply.detail()
            )));
        }
        let body = reply.body.map_err(|err| {
            BackendError::OutcomeUnknown(format!(
                "confirm-upload returned {} but its body was unreadable: {err}",
                reply.status
            ))
        })?;
        let parsed: ConfirmResponse = serde_json::from_str(&body).map_err(|e| {
            BackendError::OutcomeUnknown(format!("invalid confirm-upload receipt: {e}"))
        })?;
        if !parsed.success {
            return Err(BackendError::Schema(
                parsed
                    .detail
                    .or(parsed.message)
                    .unwrap_or_else(|| "confirm-upload returned success=false".to_string()),
            ));
        }
        let token = parsed.token.ok_or_else(|| {
            BackendError::OutcomeUnknown("confirm-upload succeeded without a token".to_string())
        })?;
        Ok(SubmitReceipt { token })
    }
}

/// A finished review carries the rendered `sections` or the full `content`; anything
/// else is not a review, whatever its status.
fn has_review_content(payload: &Value) -> bool {
    payload.get("sections").is_some_and(Value::is_object)
        || payload.get("content").is_some_and(Value::is_string)
}

#[async_trait]
impl ReviewBackend for StanfordBackend {
    fn name(&self) -> &'static str {
        "stanford"
    }

    async fn submit(&self, req: SubmitRequest) -> Result<SubmitReceipt, BackendError> {
        let file_name = upload_file_name(&req.pdf_path)
            .ok_or_else(|| BackendError::Schema("invalid PDF filename".to_string()))?;
        let venue = req.venue.unwrap_or_default();

        req.progress.enter(SubmitStep::UploadInit);
        let target = self.request_upload_target(&file_name, &venue).await?;
        req.progress.enter(SubmitStep::Upload);
        let s3_key = self.upload(target, &req.pdf_path, file_name).await?;
        req.progress.enter(SubmitStep::Confirm);
        self.confirm(s3_key, venue, req.email).await
    }

    async fn fetch_review(&self, token: &str) -> Result<ReviewFetchResult, BackendError> {
        let limit = self.timeouts.fetch;
        let resp = self
            .client
            .get(self.endpoint(&format!("/api/review/{token}")))
            .timeout(limit)
            .send()
            .await
            .map_err(|e| {
                BackendError::Network(format!("review request {}", send_failure(&e, limit)))
            })?;
        let reply = Reply::read(resp).await;
        match reply.status {
            StatusCode::TOO_MANY_REQUESTS => return Err(reply.rate_limited()),
            StatusCode::ACCEPTED => return Ok(ReviewFetchResult::Processing),
            StatusCode::NOT_FOUND => return Ok(ReviewFetchResult::InvalidToken),
            status if status.is_server_error() => {
                return Err(BackendError::Server {
                    status: status.as_u16(),
                    body: reply.detail(),
                });
            }
            status if !status.is_success() => {
                return Err(BackendError::Schema(format!(
                    "unexpected status {status} when fetching review: {}",
                    reply.detail()
                )));
            }
            _ => {}
        }
        let body = reply
            .body
            .map_err(|err| BackendError::Network(format!("review body unreadable: {err}")))?;
        let payload: Value = serde_json::from_str(&body)
            .map_err(|e| BackendError::Schema(format!("invalid review payload: {e}")))?;
        if !has_review_content(&payload) {
            return Err(BackendError::Schema(
                "review reply has no review content (expected `sections` or `content`)".to_string(),
            ));
        }
        Ok(ReviewFetchResult::Ready { raw_json: payload })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn provider_detail_reads_messages_validation_lists_and_raw_bodies() {
        let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/stanford");
        let fixture = |name: &str| {
            std::fs::read_to_string(fixtures.join(name))
                .unwrap_or_else(|e| panic!("fixture {name}: {e}"))
        };
        assert_eq!(
            provider_detail(&fixture("review-404.json")),
            "Invalid token or submission not found"
        );
        assert_eq!(
            provider_detail(&fixture("confirm-upload-422.json")),
            "s3_key: Field required; email: Field required"
        );
        assert_eq!(
            provider_detail(&fixture("get-upload-url-422.json")),
            "filename: Field required"
        );
        assert_eq!(
            provider_detail(" <html>bad gateway</html>\n"),
            "<html>bad gateway</html>"
        );
        assert_eq!(provider_detail(r#"{"detail": 7}"#), "7");
    }

    #[test]
    fn review_content_needs_sections_or_content() {
        let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/stanford");
        let ready: Value = serde_json::from_str(
            &std::fs::read_to_string(fixtures.join("review-200.json")).expect("fixture"),
        )
        .expect("fixture is JSON");
        assert!(has_review_content(&ready));
        assert!(has_review_content(&json!({ "content": "text" })));
        assert!(!has_review_content(
            &json!({ "detail": "Review is still being processed" })
        ));
        assert!(!has_review_content(&json!({ "sections": "not an object" })));
    }
}
