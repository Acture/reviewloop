use super::{BackendError, ReviewBackend, ReviewFetchResult, SubmitReceipt, SubmitRequest};
use async_trait::async_trait;
use reqwest::{Client, StatusCode, multipart};
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;

#[derive(Clone)]
pub struct StanfordBackend {
    client: Client,
    base_url: String,
}

impl StanfordBackend {
    pub fn new(base_url: String) -> Self {
        Self {
            client: Client::new(),
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
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

#[derive(Debug, Deserialize)]
struct ConfirmResponse {
    success: bool,
    token: Option<String>,
    detail: Option<String>,
    message: Option<String>,
}

#[async_trait]
impl ReviewBackend for StanfordBackend {
    fn name(&self) -> &'static str {
        "stanford"
    }

    async fn submit(&self, req: SubmitRequest) -> Result<SubmitReceipt, BackendError> {
        let file_name = req
            .pdf_path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| BackendError::Schema("invalid PDF filename".to_string()))?
            .to_string();

        let upload_url_resp = self
            .client
            .post(self.endpoint("/api/get-upload-url"))
            .json(&json!({
                "filename": file_name,
                "venue": req.venue.clone().unwrap_or_default(),
            }))
            .send()
            .await
            .map_err(|e| BackendError::Network(e.to_string()))?;

        let status = upload_url_resp.status();
        let body_text = upload_url_resp
            .text()
            .await
            .unwrap_or_else(|_| "".to_string());

        if status == StatusCode::TOO_MANY_REQUESTS {
            return Err(BackendError::RateLimited(body_text));
        }
        if status.is_server_error() {
            return Err(BackendError::Server {
                status: status.as_u16(),
                body: body_text,
            });
        }
        if !status.is_success() {
            return Err(BackendError::Schema(format!(
                "get-upload-url failed ({status}): {body_text}"
            )));
        }

        let parsed: UploadUrlResponse = serde_json::from_str(&body_text)
            .map_err(|e| BackendError::Schema(format!("invalid get-upload-url payload: {e}")))?;

        if !parsed.success {
            return Err(BackendError::Schema(parsed.detail.unwrap_or_else(|| {
                "get-upload-url returned success=false".to_string()
            })));
        }

        let presigned_url = parsed
            .presigned_url
            .ok_or_else(|| BackendError::Schema("missing presigned_url".to_string()))?;
        let s3_key = parsed
            .s3_key
            .ok_or_else(|| BackendError::Schema("missing s3_key".to_string()))?;
        let presigned_fields = parsed
            .presigned_fields
            .ok_or_else(|| BackendError::Schema("missing presigned_fields".to_string()))?;

        let file_bytes = tokio::fs::read(&req.pdf_path)
            .await
            .map_err(|e| BackendError::Network(format!("failed to read PDF: {e}")))?;

        let mut form = multipart::Form::new();
        for (k, v) in presigned_fields {
            form = form.text(k, v);
        }

        let file_part = multipart::Part::bytes(file_bytes)
            .file_name(file_name)
            .mime_str("application/pdf")
            .map_err(|e| BackendError::Schema(format!("invalid mime: {e}")))?;
        form = form.part("file", file_part);

        let s3_resp = self
            .client
            .post(presigned_url)
            .multipart(form)
            .send()
            .await
            .map_err(|e| BackendError::Network(format!("S3 upload failed: {e}")))?;

        if !(s3_resp.status().is_success() || s3_resp.status() == StatusCode::NO_CONTENT) {
            return Err(BackendError::Server {
                status: s3_resp.status().as_u16(),
                body: s3_resp
                    .text()
                    .await
                    .unwrap_or_else(|_| "S3 upload failed".to_string()),
            });
        }

        let confirm_form = multipart::Form::new()
            .text("s3_key", s3_key)
            .text("venue", req.venue.unwrap_or_default())
            .text("email", req.email);

        let confirm_resp = self
            .client
            .post(self.endpoint("/api/confirm-upload"))
            .multipart(confirm_form)
            .send()
            .await
            .map_err(|e| BackendError::Network(e.to_string()))?;

        let status = confirm_resp.status();
        let body_text = confirm_resp.text().await.unwrap_or_else(|_| "".to_string());

        if status == StatusCode::TOO_MANY_REQUESTS {
            return Err(BackendError::RateLimited(body_text));
        }
        if status.is_server_error() {
            return Err(BackendError::Server {
                status: status.as_u16(),
                body: body_text,
            });
        }
        if !status.is_success() {
            return Err(BackendError::Schema(format!(
                "confirm-upload failed ({status}): {body_text}"
            )));
        }

        let parsed: ConfirmResponse = serde_json::from_str(&body_text)
            .map_err(|e| BackendError::Schema(format!("invalid confirm payload: {e}")))?;

        if !parsed.success {
            return Err(BackendError::Schema(
                parsed
                    .detail
                    .or(parsed.message)
                    .unwrap_or_else(|| "confirm-upload returned success=false".to_string()),
            ));
        }

        let token = parsed
            .token
            .ok_or_else(|| BackendError::Schema("confirm-upload missing token".to_string()))?;

        Ok(SubmitReceipt {
            token,
            backend_submission_ref: None,
        })
    }

    async fn fetch_review(&self, token: &str) -> Result<ReviewFetchResult, BackendError> {
        let resp = self
            .client
            .get(self.endpoint(&format!("/api/review/{token}")))
            .send()
            .await
            .map_err(|e| BackendError::Network(e.to_string()))?;

        let status = resp.status();

        if status == StatusCode::TOO_MANY_REQUESTS {
            return Err(BackendError::RateLimited(
                resp.text()
                    .await
                    .unwrap_or_else(|_| "rate limited".to_string()),
            ));
        }

        if status == StatusCode::ACCEPTED {
            return Ok(ReviewFetchResult::Processing);
        }

        if status == StatusCode::NOT_FOUND {
            return Ok(ReviewFetchResult::InvalidToken);
        }

        if status.is_server_error() {
            return Err(BackendError::Server {
                status: status.as_u16(),
                body: resp
                    .text()
                    .await
                    .unwrap_or_else(|_| "server error".to_string()),
            });
        }

        if !status.is_success() {
            return Err(BackendError::Schema(format!(
                "unexpected status {} when fetching review",
                status.as_u16()
            )));
        }

        let payload = resp
            .json::<serde_json::Value>()
            .await
            .map_err(|e| BackendError::Schema(format!("invalid review payload: {e}")))?;

        Ok(ReviewFetchResult::Ready { raw_json: payload })
    }
}
