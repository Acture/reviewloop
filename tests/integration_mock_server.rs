use anyhow::{Context, Result};
use axum::{
    Router,
    body::Body,
    body::to_bytes,
    extract::{Path as AxPath, State},
    http::{Request, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::Utc;
use reviewloop::{
    config::{Config, PaperConfig},
    db::Db,
    model::{Job, JobPdf, JobStatus, NewJob},
    submission_input::prepare_input,
    util::sha256_file,
    worker,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, VecDeque},
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, Mutex},
};
use tempfile::TempDir;
use tokio::{net::TcpListener, task::JoinHandle};

#[derive(Clone, Debug)]
struct MockReply {
    status: StatusCode,
    body: String,
    content_type: &'static str,
    extra_headers: Vec<(&'static str, String)>,
}

impl MockReply {
    fn json(status: StatusCode, value: serde_json::Value) -> Self {
        Self {
            status,
            body: value.to_string(),
            content_type: "application/json",
            extra_headers: vec![],
        }
    }

    fn text(status: StatusCode, body: impl Into<String>) -> Self {
        Self {
            status,
            body: body.into(),
            content_type: "text/plain",
            extra_headers: vec![],
        }
    }

    fn with_retry_after_secs(mut self, secs: u64) -> Self {
        self.extra_headers.push(("retry-after", secs.to_string()));
        self
    }

    fn default_get_upload_error() -> Self {
        Self::json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"detail": "mock get-upload-url response queue is empty"}),
        )
    }

    fn default_confirm_error() -> Self {
        Self::json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"detail": "mock confirm-upload response queue is empty"}),
        )
    }

    fn default_s3_error() -> Self {
        Self::text(
            StatusCode::INTERNAL_SERVER_ERROR,
            "mock s3 response queue is empty",
        )
    }

    fn default_review_invalid() -> Self {
        Self::json(
            StatusCode::NOT_FOUND,
            json!({"detail": "Invalid token or submission not found"}),
        )
    }
}

impl IntoResponse for MockReply {
    fn into_response(self) -> Response {
        let mut response = (self.status, self.body).into_response();
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static(self.content_type),
        );
        for (name, value) in &self.extra_headers {
            if let (Ok(header_name), Ok(header_value)) = (
                header::HeaderName::from_bytes(name.as_bytes()),
                header::HeaderValue::from_str(value),
            ) {
                response.headers_mut().insert(header_name, header_value);
            }
        }
        response
    }
}

#[derive(Default)]
struct MockState {
    get_upload_queue: Mutex<VecDeque<MockReply>>,
    confirm_queue: Mutex<VecDeque<MockReply>>,
    s3_queue: Mutex<VecDeque<MockReply>>,
    review_queue: Mutex<HashMap<String, VecDeque<MockReply>>>,
    calls: Mutex<HashMap<String, usize>>,
    /// File part of every multipart body received by the mock S3 endpoint.
    s3_uploads: Mutex<Vec<Vec<u8>>>,
}

impl MockState {
    fn enqueue_get_upload(&self, reply: MockReply) {
        self.get_upload_queue.lock().unwrap().push_back(reply);
    }

    fn enqueue_confirm(&self, reply: MockReply) {
        self.confirm_queue.lock().unwrap().push_back(reply);
    }

    fn enqueue_s3(&self, reply: MockReply) {
        self.s3_queue.lock().unwrap().push_back(reply);
    }

    fn enqueue_review(&self, token: &str, reply: MockReply) {
        self.review_queue
            .lock()
            .unwrap()
            .entry(token.to_string())
            .or_default()
            .push_back(reply);
    }

    fn pop_get_upload(&self) -> MockReply {
        self.get_upload_queue
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(MockReply::default_get_upload_error)
    }

    fn pop_confirm(&self) -> MockReply {
        self.confirm_queue
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(MockReply::default_confirm_error)
    }

    fn pop_s3(&self) -> MockReply {
        self.s3_queue
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(MockReply::default_s3_error)
    }

    fn pop_review(&self, token: &str) -> MockReply {
        self.review_queue
            .lock()
            .unwrap()
            .get_mut(token)
            .and_then(|queue| queue.pop_front())
            .unwrap_or_else(MockReply::default_review_invalid)
    }

    fn mark_call(&self, route: &str) {
        let mut calls = self.calls.lock().unwrap();
        *calls.entry(route.to_string()).or_insert(0) += 1;
    }

    fn call_count(&self, route: &str) -> usize {
        self.calls.lock().unwrap().get(route).copied().unwrap_or(0)
    }

    fn s3_uploads(&self) -> Vec<Vec<u8>> {
        self.s3_uploads.lock().unwrap().clone()
    }
}

fn find_bytes(haystack: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    haystack
        .get(from..)?
        .windows(needle.len())
        .position(|window| window == needle)
        .map(|offset| offset + from)
}

/// Extract the `file` part from a `multipart/form-data` body.
fn multipart_file_part(content_type: &str, body: &[u8]) -> Option<Vec<u8>> {
    let boundary = content_type.split("boundary=").nth(1)?.trim_matches('"');
    let header_at = find_bytes(body, b"name=\"file\"", 0)?;
    let content_start = find_bytes(body, b"\r\n\r\n", header_at)? + 4;
    let content_end = find_bytes(body, format!("\r\n--{boundary}").as_bytes(), content_start)?;
    Some(body[content_start..content_end].to_vec())
}

struct MockServer {
    base_url: String,
    handle: JoinHandle<()>,
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl MockServer {
    async fn start(state: Arc<MockState>) -> Result<Self> {
        let app = Router::new()
            .route("/api/get-upload-url", post(handle_get_upload))
            .route("/api/confirm-upload", post(handle_confirm_upload))
            .route("/api/review/{token}", get(handle_review))
            .route("/s3/upload", post(handle_s3_upload))
            .with_state(state);

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;

        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        Ok(Self {
            base_url: format!("http://{}", addr),
            handle,
        })
    }
}

async fn handle_get_upload(
    State(state): State<Arc<MockState>>,
    req: Request<Body>,
) -> impl IntoResponse {
    state.mark_call("get_upload");
    let _ = to_bytes(req.into_body(), usize::MAX).await;
    state.pop_get_upload()
}

async fn handle_confirm_upload(
    State(state): State<Arc<MockState>>,
    req: Request<Body>,
) -> impl IntoResponse {
    state.mark_call("confirm_upload");
    let _ = to_bytes(req.into_body(), usize::MAX).await;
    state.pop_confirm()
}

async fn handle_s3_upload(
    State(state): State<Arc<MockState>>,
    req: Request<Body>,
) -> impl IntoResponse {
    state.mark_call("s3_upload");
    let content_type = req
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    if let Ok(body) = to_bytes(req.into_body(), usize::MAX).await
        && let Some(file) = multipart_file_part(&content_type, &body)
    {
        state.s3_uploads.lock().unwrap().push(file);
    }
    state.pop_s3()
}

async fn handle_review(
    State(state): State<Arc<MockState>>,
    AxPath(token): AxPath<String>,
) -> impl IntoResponse {
    state.mark_call(&format!("review:{token}"));
    state.pop_review(&token)
}

struct TestContext {
    _tmp: TempDir,
    config: Config,
    db: Db,
    pdf_path: PathBuf,
}

impl TestContext {
    fn new(base_url: String) -> Result<Self> {
        let tmp = tempfile::tempdir()?;
        let state_dir = tmp.path().join("state");
        fs::create_dir_all(&state_dir)?;

        let pdf_path = tmp.path().join("paper.pdf");
        fs::write(&pdf_path, b"%PDF-1.4\n1 0 obj\n<<>>\nendobj\n%%EOF\n")?;

        let mut config = Config {
            project_id: "project-integration".to_string(),
            ..Config::default()
        };
        config.core.state_dir = state_dir.to_string_lossy().to_string();
        config.core.max_concurrency = 2;
        config.polling.schedule_minutes = vec![10, 20, 40, 60];
        config.polling.jitter_percent = 0;
        config.trigger.git.enabled = false;
        config.trigger.pdf.enabled = false;
        config.imap = None;
        config.providers.stanford.base_url = base_url;
        config.providers.stanford.email = "test@example.edu".to_string();
        config.providers.stanford.venue = Some("ICLR".to_string());
        config.papers = vec![PaperConfig {
            id: "main".to_string(),
            pdf_path: pdf_path.to_string_lossy().to_string(),
            backend: "stanford".to_string(),
            venue: None,
        }];

        let db = Db::new(Path::new(&config.core.state_dir));
        db.ensure_schema()?;

        Ok(Self {
            _tmp: tmp,
            config,
            db,
            pdf_path,
        })
    }

    fn set_fallback_script(&mut self, script_body: &str) -> Result<()> {
        let script_path = Path::new(&self.config.core.state_dir).join("fallback_success.js");
        fs::write(&script_path, script_body)?;
        self.config.providers.stanford.fallback_script = script_path.to_string_lossy().to_string();
        Ok(())
    }

    fn create_job(&self, status: JobStatus) -> Result<Job> {
        let pdf_hash = sha256_file(&self.pdf_path)?;

        self.db.create_job(&NewJob {
            project_id: self.config.project_id.clone(),
            paper_id: "main".to_string(),
            backend: "stanford".to_string(),
            pdf: JobPdf::Unpinned {
                pdf_path: self.pdf_path.to_string_lossy().to_string(),
                pdf_hash,
            },
            status,
            email: self.config.providers.stanford.email.clone(),
            venue: self.config.providers.stanford.venue.clone(),
            git_tag: None,
            git_commit: None,
            next_poll_at: None,
        })
    }

    /// Enqueue the way the CLI and triggers do: snapshot first, then insert.
    fn create_pinned_job(&self, status: JobStatus) -> Result<Job> {
        let input = prepare_input(&self.config.state_dir(), &self.pdf_path)?;
        self.db.create_job(&NewJob {
            project_id: self.config.project_id.clone(),
            paper_id: "main".to_string(),
            backend: "stanford".to_string(),
            pdf: JobPdf::Pinned(input),
            status,
            email: self.config.providers.stanford.email.clone(),
            venue: self.config.providers.stanford.venue.clone(),
            git_tag: None,
            git_commit: None,
            next_poll_at: None,
        })
    }

    fn create_processing_job(&self, token: &str) -> Result<Job> {
        let job = self.create_job(JobStatus::Queued)?;
        self.db.attach_token_to_job(&job.id, token, Utc::now())?;
        self.db
            .get_job(&job.id)?
            .context("failed to load processing job")
    }
}

fn assert_cooldown_minutes(value: chrono::DateTime<Utc>, min: i64, max: i64) {
    let delta = value - Utc::now();
    let minutes = delta.num_minutes();
    assert!(
        (min..=max).contains(&minutes),
        "expected cooldown in [{min}, {max}] minutes, got {minutes}"
    );
}

#[tokio::test]
async fn integration_submit_chain_get_upload_s3_confirm_success() -> Result<()> {
    let state = Arc::new(MockState::default());
    let server = MockServer::start(state.clone()).await?;

    state.enqueue_get_upload(MockReply::json(
        StatusCode::OK,
        json!({
            "success": true,
            "presigned_url": format!("{}/s3/upload", server.base_url),
            "s3_key": "uploads/test.pdf",
            "presigned_fields": {
                "key": "uploads/test.pdf",
                "policy": "abc",
                "x-amz-algorithm": "AWS4-HMAC-SHA256",
                "x-amz-credential": "credential",
                "x-amz-date": "20260304T000000Z",
                "x-amz-signature": "sig"
            }
        }),
    ));
    state.enqueue_s3(MockReply::text(StatusCode::NO_CONTENT, ""));
    state.enqueue_confirm(MockReply::json(
        StatusCode::OK,
        json!({ "success": true, "token": "tok-submit-success" }),
    ));

    let ctx = TestContext::new(server.base_url.clone())?;
    let job = ctx.create_job(JobStatus::Queued)?;

    worker::submit_job(&ctx.config, &ctx.db, &job.id).await?;

    let updated = ctx.db.get_job(&job.id)?.context("job not found")?;
    assert_eq!(updated.status, JobStatus::Processing);
    assert_eq!(updated.token.as_deref(), Some("tok-submit-success"));

    assert_eq!(state.call_count("get_upload"), 1);
    assert_eq!(state.call_count("s3_upload"), 1);
    assert_eq!(state.call_count("confirm_upload"), 1);

    Ok(())
}

#[tokio::test]
async fn integration_poll_transitions_from_202_to_200_and_writes_artifacts() -> Result<()> {
    let state = Arc::new(MockState::default());
    let server = MockServer::start(state.clone()).await?;

    state.enqueue_review(
        "tok-202-200",
        MockReply::json(
            StatusCode::ACCEPTED,
            json!({ "detail": "still processing" }),
        ),
    );
    state.enqueue_review(
        "tok-202-200",
        MockReply::json(
            StatusCode::OK,
            json!({
                "title": "Sample Paper",
                "venue": "ICLR",
                "sections": {
                    "summary": "Solid work",
                    "strengths": "Strong baseline",
                    "weaknesses": "Limited ablation"
                }
            }),
        ),
    );

    let ctx = TestContext::new(server.base_url.clone())?;
    let job = ctx.create_processing_job("tok-202-200")?;

    worker::poll_job(&ctx.config, &ctx.db, &job).await?;
    let first = ctx
        .db
        .get_job(&job.id)?
        .context("job not found after first poll")?;
    assert_eq!(first.status, JobStatus::Processing);
    assert_eq!(first.attempt, 1);

    worker::poll_job(&ctx.config, &ctx.db, &first).await?;
    let done = ctx
        .db
        .get_job(&job.id)?
        .context("job not found after second poll")?;
    assert_eq!(done.status, JobStatus::Completed);

    let artifact_root = Path::new(&ctx.config.core.state_dir)
        .join("artifacts")
        .join(&job.id);
    assert!(artifact_root.join("review.json").exists());
    assert!(artifact_root.join("review.md").exists());
    assert!(artifact_root.join("meta.json").exists());

    Ok(())
}

#[tokio::test]
async fn integration_poll_404_marks_job_failed_invalid_token() -> Result<()> {
    let state = Arc::new(MockState::default());
    let server = MockServer::start(state.clone()).await?;

    state.enqueue_review(
        "tok-invalid",
        MockReply::json(
            StatusCode::NOT_FOUND,
            json!({"detail": "Invalid token or submission not found"}),
        ),
    );

    let ctx = TestContext::new(server.base_url.clone())?;
    let job = ctx.create_processing_job("tok-invalid")?;

    worker::poll_job(&ctx.config, &ctx.db, &job).await?;

    let failed = ctx.db.get_job(&job.id)?.context("job not found")?;
    assert_eq!(failed.status, JobStatus::Failed);
    assert_eq!(failed.last_error.as_deref(), Some("invalid token"));

    Ok(())
}

// Previously asserted a 30-minute hard cooldown; now the behavior is:
//  - 429 with Retry-After header  → next_poll_at = now + header duration
//  - 429 without Retry-After      → next_poll_at = compute_next_poll_at (normal schedule)
//  - 5xx non-terminal             → next_poll_at = compute_next_poll_at (normal schedule)
#[tokio::test]
async fn integration_rate_limit_honors_retry_after_header() -> Result<()> {
    let state = Arc::new(MockState::default());
    let server = MockServer::start(state.clone()).await?;

    // 429 on submit with Retry-After: 5 seconds
    state.enqueue_get_upload(
        MockReply::json(
            StatusCode::TOO_MANY_REQUESTS,
            json!({ "detail": "rate limit exceeded" }),
        )
        .with_retry_after_secs(5),
    );

    let ctx = TestContext::new(server.base_url.clone())?;
    let submit_job = ctx.create_job(JobStatus::Queued)?;
    let before = Utc::now();

    worker::submit_job(&ctx.config, &ctx.db, &submit_job.id).await?;

    let queued = ctx
        .db
        .get_job(&submit_job.id)?
        .context("submit job missing")?;
    assert_eq!(queued.status, JobStatus::Queued);
    assert_eq!(queued.attempt, 1);
    let next = queued.next_poll_at.context("missing next_poll_at")?;
    let delta = (next - before).num_seconds();
    assert!(
        (3..=10).contains(&delta),
        "expected ~5s Retry-After scheduling, got {delta}s"
    );

    // 429 on poll with Retry-After: 5 seconds
    state.enqueue_review(
        "tok-rl-header",
        MockReply::json(
            StatusCode::TOO_MANY_REQUESTS,
            json!({ "detail": "rate limited" }),
        )
        .with_retry_after_secs(5),
    );

    let poll_job = ctx.create_processing_job("tok-rl-header")?;
    let before2 = Utc::now();

    worker::poll_job(&ctx.config, &ctx.db, &poll_job).await?;

    let retrying = ctx.db.get_job(&poll_job.id)?.context("poll job missing")?;
    assert_eq!(retrying.status, JobStatus::Processing);
    let next2 = retrying.next_poll_at.context("missing next_poll_at")?;
    let delta2 = (next2 - before2).num_seconds();
    assert!(
        (3..=10).contains(&delta2),
        "expected ~5s Retry-After scheduling, got {delta2}s"
    );

    Ok(())
}

#[tokio::test]
async fn integration_rate_limit_without_retry_after_falls_back_to_schedule() -> Result<()> {
    let state = Arc::new(MockState::default());
    let server = MockServer::start(state.clone()).await?;

    // 429 without Retry-After header — should use polling schedule (10-60 min range)
    state.enqueue_get_upload(MockReply::json(
        StatusCode::TOO_MANY_REQUESTS,
        json!({ "detail": "rate limit exceeded" }),
    ));

    let ctx = TestContext::new(server.base_url.clone())?;
    let submit_job = ctx.create_job(JobStatus::Queued)?;

    worker::submit_job(&ctx.config, &ctx.db, &submit_job.id).await?;

    let queued = ctx
        .db
        .get_job(&submit_job.id)?
        .context("submit job missing")?;
    assert_eq!(queued.status, JobStatus::Queued);
    assert_eq!(queued.attempt, 1);
    // schedule = [10,20,40,60], attempt+1=1 → index 1 → 20 minutes (jitter=0)
    assert_cooldown_minutes(queued.next_poll_at.context("missing next_poll_at")?, 19, 21);

    Ok(())
}

#[tokio::test]
async fn integration_server_error_uses_polling_schedule() -> Result<()> {
    let state = Arc::new(MockState::default());
    let server = MockServer::start(state.clone()).await?;

    state.enqueue_review(
        "tok-500-sched",
        MockReply::text(StatusCode::INTERNAL_SERVER_ERROR, "backend overloaded"),
    );

    let ctx = TestContext::new(server.base_url.clone())?;
    let poll_job = ctx.create_processing_job("tok-500-sched")?;

    worker::poll_job(&ctx.config, &ctx.db, &poll_job).await?;

    let retrying = ctx.db.get_job(&poll_job.id)?.context("poll job missing")?;
    assert_eq!(retrying.status, JobStatus::Processing);
    assert_eq!(retrying.attempt, 1);
    // schedule = [10,20,40,60], attempt+1=1 → index 1 → 20 minutes (jitter=0)
    // NOT 30 minutes (the old hard cooldown)
    assert_cooldown_minutes(
        retrying.next_poll_at.context("missing next_poll_at")?,
        19,
        21,
    );
    assert!(
        retrying
            .last_error
            .as_deref()
            .unwrap_or_default()
            .contains("backend overloaded")
    );

    Ok(())
}

#[tokio::test]
async fn integration_poll_terminal_generation_error_marks_failed_needs_manual() -> Result<()> {
    let state = Arc::new(MockState::default());
    let server = MockServer::start(state.clone()).await?;

    state.enqueue_review(
        "tok-terminal",
        MockReply::json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"detail": "Review generation failed. Please contact support."}),
        ),
    );

    let ctx = TestContext::new(server.base_url.clone())?;
    let poll_job = ctx.create_processing_job("tok-terminal")?;

    worker::poll_job(&ctx.config, &ctx.db, &poll_job).await?;

    let failed = ctx.db.get_job(&poll_job.id)?.context("poll job missing")?;
    assert_eq!(failed.status, JobStatus::FailedNeedsManual);
    assert_eq!(failed.attempt, 1);
    assert!(failed.next_poll_at.is_none());
    assert!(
        failed
            .last_error
            .as_deref()
            .unwrap_or_default()
            .contains("Review generation failed")
    );
    assert_eq!(state.call_count("review:tok-terminal"), 1);

    Ok(())
}

#[tokio::test]
async fn integration_submit_uses_fallback_and_persists_token() -> Result<()> {
    let node_available = Command::new("node")
        .arg("--version")
        .output()
        .map(|out| out.status.success())
        .unwrap_or(false);
    if !node_available {
        return Ok(());
    }

    let state = Arc::new(MockState::default());
    let server = MockServer::start(state.clone()).await?;

    state.enqueue_get_upload(MockReply::json(
        StatusCode::INTERNAL_SERVER_ERROR,
        json!({ "detail": "forced primary failure" }),
    ));

    let mut ctx = TestContext::new(server.base_url.clone())?;
    ctx.set_fallback_script(
        r#"#!/usr/bin/env node
console.log(JSON.stringify({ success: true, token: "fallback-token-123" }));
"#,
    )?;

    let job = ctx.create_job(JobStatus::Queued)?;

    worker::submit_job(&ctx.config, &ctx.db, &job.id).await?;

    let updated = ctx.db.get_job(&job.id)?.context("job not found")?;
    assert_eq!(updated.status, JobStatus::Processing);
    assert_eq!(updated.token.as_deref(), Some("fallback-token-123"));
    assert!(updated.fallback_used);

    Ok(())
}

#[tokio::test]
async fn integration_virtual_paper_e2e_single_tick_completes() -> Result<()> {
    let state = Arc::new(MockState::default());
    let server = MockServer::start(state.clone()).await?;

    state.enqueue_get_upload(MockReply::json(
        StatusCode::OK,
        json!({
            "success": true,
            "presigned_url": format!("{}/s3/upload", server.base_url),
            "s3_key": "uploads/virtual-paper.pdf",
            "presigned_fields": {
                "key": "uploads/virtual-paper.pdf",
                "policy": "abc",
                "x-amz-algorithm": "AWS4-HMAC-SHA256",
                "x-amz-credential": "credential",
                "x-amz-date": "20260305T000000Z",
                "x-amz-signature": "sig"
            }
        }),
    ));
    state.enqueue_s3(MockReply::text(StatusCode::NO_CONTENT, ""));
    state.enqueue_confirm(MockReply::json(
        StatusCode::OK,
        json!({ "success": true, "token": "tok-virtual-e2e" }),
    ));
    state.enqueue_review(
        "tok-virtual-e2e",
        MockReply::json(
            StatusCode::OK,
            json!({
                "title": "Virtual Paper",
                "sections": {
                    "summary": "End-to-end mock loop passed",
                    "strengths": "Automated submission and retrieval",
                    "weaknesses": "Synthetic content only"
                }
            }),
        ),
    );

    let mut ctx = TestContext::new(server.base_url.clone())?;
    // Make the input explicit in the test: this is a synthetic paper fixture.
    fs::write(
        &ctx.pdf_path,
        b"%PDF-1.4\n%VirtualPaper\n1 0 obj\n<< /Title (Virtual Paper) >>\nendobj\n%%EOF\n",
    )?;
    ctx.config.polling.schedule_minutes = vec![0];

    let job = ctx.create_job(JobStatus::Queued)?;
    worker::run_tick(&ctx.config, &ctx.db).await?;

    let done = ctx
        .db
        .get_job(&job.id)?
        .context("virtual e2e job not found")?;
    assert_eq!(done.status, JobStatus::Completed);
    assert_eq!(done.token.as_deref(), Some("tok-virtual-e2e"));

    let artifact_root = Path::new(&ctx.config.core.state_dir)
        .join("artifacts")
        .join(&job.id);
    assert!(artifact_root.join("review.json").exists());
    assert!(artifact_root.join("review.md").exists());
    assert!(artifact_root.join("meta.json").exists());

    assert_eq!(state.call_count("get_upload"), 1);
    assert_eq!(state.call_count("s3_upload"), 1);
    assert_eq!(state.call_count("confirm_upload"), 1);
    assert_eq!(state.call_count("review:tok-virtual-e2e"), 1);

    Ok(())
}

/// Validates the full `reviewloop run` loop behavior:
/// - submit with force (QUEUED → SUBMITTED → PROCESSING)
/// - drive run_tick in a loop until the job reaches a terminal state
/// - assert COMPLETED and artifacts present
/// - assert exit-code semantics (terminal detection)
#[tokio::test]
async fn integration_run_loop_completes_via_ticks() -> Result<()> {
    let state = Arc::new(MockState::default());
    let server = MockServer::start(state.clone()).await?;

    const TOKEN: &str = "tok-run-loop";

    state.enqueue_get_upload(MockReply::json(
        StatusCode::OK,
        json!({
            "success": true,
            "presigned_url": format!("{}/s3/upload", server.base_url),
            "s3_key": "uploads/run-loop.pdf",
            "presigned_fields": {
                "key": "uploads/run-loop.pdf",
                "policy": "abc",
                "x-amz-algorithm": "AWS4-HMAC-SHA256",
                "x-amz-credential": "credential",
                "x-amz-date": "20260401T000000Z",
                "x-amz-signature": "sig"
            }
        }),
    ));
    state.enqueue_s3(MockReply::text(StatusCode::NO_CONTENT, ""));
    state.enqueue_confirm(MockReply::json(
        StatusCode::OK,
        json!({ "success": true, "token": TOKEN }),
    ));
    // First poll: still processing.
    state.enqueue_review(
        TOKEN,
        MockReply::json(
            StatusCode::ACCEPTED,
            json!({ "detail": "still processing" }),
        ),
    );
    // Second poll: review ready.
    state.enqueue_review(
        TOKEN,
        MockReply::json(
            StatusCode::OK,
            json!({
                "title": "Run Loop Paper",
                "sections": {
                    "summary": "The run loop drove this to completion",
                    "strengths": "Automated foreground polling",
                    "weaknesses": "None"
                }
            }),
        ),
    );

    let mut ctx = TestContext::new(server.base_url.clone())?;
    ctx.config.polling.schedule_minutes = vec![0];

    // Step 1: simulate cmd_run's force-submit (create QUEUED job + submit immediately).
    let job = ctx.create_job(JobStatus::Queued)?;
    worker::submit_job(&ctx.config, &ctx.db, &job.id).await?;

    // After submit, job should be PROCESSING with a token.
    let submitted = ctx
        .db
        .get_job(&job.id)?
        .context("job not found after submit")?;
    assert_eq!(submitted.status, JobStatus::Processing);
    assert_eq!(submitted.token.as_deref(), Some(TOKEN));

    // Step 2: drive the polling loop until terminal (mirrors the cmd_run loop body).
    let mut reached_terminal = false;
    for _ in 0..10 {
        worker::run_tick(&ctx.config, &ctx.db).await?;
        let updated = ctx.db.get_job(&job.id)?.context("job disappeared")?;
        if matches!(
            updated.status,
            JobStatus::Completed
                | JobStatus::Failed
                | JobStatus::FailedNeedsManual
                | JobStatus::Timeout
        ) {
            reached_terminal = true;
            break;
        }
    }

    assert!(
        reached_terminal,
        "job should reach terminal state within 10 ticks"
    );

    let final_job = ctx.db.get_job(&job.id)?.context("final job not found")?;
    assert_eq!(
        final_job.status,
        JobStatus::Completed,
        "expected COMPLETED, got {}",
        final_job.status.as_str()
    );

    // Step 3: verify artifacts (what cmd_run would report on Completed).
    let artifact_root = Path::new(&ctx.config.core.state_dir)
        .join("artifacts")
        .join(&job.id);
    assert!(
        artifact_root.join("review.json").exists(),
        "review.json missing"
    );
    assert!(
        artifact_root.join("review.md").exists(),
        "review.md missing"
    );
    assert!(
        artifact_root.join("meta.json").exists(),
        "meta.json missing"
    );

    // Step 4: verify exit-code semantics by checking that Completed maps to code 0,
    // and non-Completed terminal statuses would map to code 2.
    let exit_code = match final_job.status {
        JobStatus::Completed => 0u32,
        JobStatus::Failed | JobStatus::FailedNeedsManual | JobStatus::Timeout => 2,
        _ => 99, // non-terminal: unexpected
    };
    assert_eq!(exit_code, 0, "expected exit code 0 for Completed");

    assert_eq!(state.call_count("get_upload"), 1);
    assert_eq!(state.call_count("s3_upload"), 1);
    assert_eq!(state.call_count("confirm_upload"), 1);
    assert_eq!(state.call_count(&format!("review:{TOKEN}")), 2); // 1 processing + 1 ready

    Ok(())
}

/// Validates that a terminal-failure job would map to exit code 2.
#[tokio::test]
async fn integration_run_loop_terminal_failure_exit_code_2() -> Result<()> {
    let state = Arc::new(MockState::default());
    let server = MockServer::start(state.clone()).await?;

    const TOKEN: &str = "tok-run-fail";

    state.enqueue_get_upload(MockReply::json(
        StatusCode::OK,
        json!({
            "success": true,
            "presigned_url": format!("{}/s3/upload", server.base_url),
            "s3_key": "uploads/run-fail.pdf",
            "presigned_fields": {
                "key": "uploads/run-fail.pdf",
                "policy": "abc",
                "x-amz-algorithm": "AWS4-HMAC-SHA256",
                "x-amz-credential": "credential",
                "x-amz-date": "20260401T000000Z",
                "x-amz-signature": "sig"
            }
        }),
    ));
    state.enqueue_s3(MockReply::text(StatusCode::NO_CONTENT, ""));
    state.enqueue_confirm(MockReply::json(
        StatusCode::OK,
        json!({ "success": true, "token": TOKEN }),
    ));
    state.enqueue_review(
        TOKEN,
        MockReply::json(
            StatusCode::NOT_FOUND,
            json!({"detail": "Invalid token or submission not found"}),
        ),
    );

    let mut ctx = TestContext::new(server.base_url.clone())?;
    ctx.config.polling.schedule_minutes = vec![0];

    let job = ctx.create_job(JobStatus::Queued)?;
    worker::submit_job(&ctx.config, &ctx.db, &job.id).await?;
    worker::run_tick(&ctx.config, &ctx.db).await?;

    let final_job = ctx.db.get_job(&job.id)?.context("final job not found")?;
    assert_eq!(final_job.status, JobStatus::Failed);

    let exit_code = match final_job.status {
        JobStatus::Completed => 0u32,
        JobStatus::Failed | JobStatus::FailedNeedsManual | JobStatus::Timeout => 2,
        _ => 99,
    };
    assert_eq!(exit_code, 2, "expected exit code 2 for Failed");

    Ok(())
}

// ── Pinned PDF snapshots ────────────────────────────────────────────────────

const EDITED_PDF: &[u8] = b"%PDF-1.4\n%edited after enqueue\n%%EOF\n";

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn enqueue_successful_submit(state: &MockState, base_url: &str, token: &str) {
    state.enqueue_get_upload(MockReply::json(
        StatusCode::OK,
        json!({
            "success": true,
            "presigned_url": format!("{base_url}/s3/upload"),
            "s3_key": "uploads/pinned.pdf",
            "presigned_fields": { "key": "uploads/pinned.pdf" }
        }),
    ));
    state.enqueue_s3(MockReply::text(StatusCode::NO_CONTENT, ""));
    state.enqueue_confirm(MockReply::json(
        StatusCode::OK,
        json!({ "success": true, "token": token }),
    ));
}

fn read_meta(ctx: &TestContext, job_id: &str) -> Result<Value> {
    let path = ctx
        .config
        .state_dir()
        .join("artifacts")
        .join(job_id)
        .join("meta.json");
    Ok(serde_json::from_str(&fs::read_to_string(path)?)?)
}

fn job_event_types(ctx: &TestContext) -> Result<Vec<String>> {
    Ok(ctx
        .db
        .list_timeline_events(&ctx.config.project_id, "main")?
        .into_iter()
        .map(|event| event.event_type)
        .collect())
}

/// Edit the source and repoint the paper at a decoy after enqueue: neither
/// may leak into the submission.
fn mutate_source_and_config(ctx: &mut TestContext) -> Result<()> {
    fs::write(&ctx.pdf_path, EDITED_PDF)?;
    let decoy = ctx.pdf_path.with_file_name("decoy.pdf");
    fs::write(&decoy, b"%PDF-1.4\n%decoy\n%%EOF\n")?;
    ctx.config.papers[0].pdf_path = decoy.to_string_lossy().to_string();
    Ok(())
}

#[tokio::test]
async fn pinned_snapshot_is_uploaded_and_archived_after_source_and_config_change() -> Result<()> {
    let state = Arc::new(MockState::default());
    let server = MockServer::start(state.clone()).await?;
    enqueue_successful_submit(&state, &server.base_url, "tok-pinned");
    state.enqueue_review(
        "tok-pinned",
        MockReply::json(
            StatusCode::OK,
            json!({ "title": "Pinned", "content": "ok" }),
        ),
    );

    let mut ctx = TestContext::new(server.base_url.clone())?;
    let original = fs::read(&ctx.pdf_path)?;
    let job = ctx.create_pinned_job(JobStatus::Queued)?;
    let snapshot_path = job.snapshot_path.clone().context("job must be pinned")?;
    assert_eq!(job.pdf_hash, sha256_hex(&original));
    mutate_source_and_config(&mut ctx)?;

    worker::submit_job(&ctx.config, &ctx.db, &job.id).await?;

    let uploads = state.s3_uploads();
    assert_eq!(
        uploads,
        vec![original.clone()],
        "backend must receive the snapshot"
    );
    let submitted = ctx.db.get_job(&job.id)?.context("job not found")?;
    assert_eq!(submitted.status, JobStatus::Processing);

    worker::poll_job(&ctx.config, &ctx.db, &submitted).await?;
    let done = ctx.db.get_job(&job.id)?.context("job not found")?;
    assert_eq!(done.status, JobStatus::Completed);

    let meta = read_meta(&ctx, &job.id)?;
    assert_eq!(meta["pdf_hash"], json!(sha256_hex(&uploads[0])));
    assert_eq!(meta["snapshot_path"], json!(snapshot_path));
    assert_eq!(meta["pdf_path"], json!(ctx.pdf_path.to_string_lossy()));
    assert_eq!(fs::read(&snapshot_path)?, uploads[0]);
    Ok(())
}

#[tokio::test]
async fn pinned_snapshot_survives_source_deletion_and_paper_removal() -> Result<()> {
    let state = Arc::new(MockState::default());
    let server = MockServer::start(state.clone()).await?;
    enqueue_successful_submit(&state, &server.base_url, "tok-orphaned-source");

    let mut ctx = TestContext::new(server.base_url.clone())?;
    let original = fs::read(&ctx.pdf_path)?;
    let job = ctx.create_pinned_job(JobStatus::Queued)?;
    fs::remove_file(&ctx.pdf_path)?;
    ctx.config.papers.clear();

    worker::submit_job(&ctx.config, &ctx.db, &job.id).await?;

    assert_eq!(state.s3_uploads(), vec![original]);
    let submitted = ctx.db.get_job(&job.id)?.context("job not found")?;
    assert_eq!(submitted.status, JobStatus::Processing);
    assert_eq!(submitted.token.as_deref(), Some("tok-orphaned-source"));
    Ok(())
}

#[tokio::test]
async fn fallback_uploads_pinned_snapshot_after_source_and_config_change() -> Result<()> {
    let node_available = Command::new("node")
        .arg("--version")
        .output()
        .map(|out| out.status.success())
        .unwrap_or(false);
    if !node_available {
        return Ok(());
    }

    let state = Arc::new(MockState::default());
    let server = MockServer::start(state.clone()).await?;
    state.enqueue_get_upload(MockReply::json(
        StatusCode::INTERNAL_SERVER_ERROR,
        json!({ "detail": "forced primary failure" }),
    ));

    let mut ctx = TestContext::new(server.base_url.clone())?;
    // The token reports what the fallback actually read: "<sha256>|<path>".
    ctx.set_fallback_script(
        r#"#!/usr/bin/env node
const fs = require("fs");
const crypto = require("crypto");
const pdf = process.argv[process.argv.indexOf("--pdf") + 1];
const hash = crypto.createHash("sha256").update(fs.readFileSync(pdf)).digest("hex");
console.log(JSON.stringify({ success: true, token: `${hash}|${pdf}` }));
"#,
    )?;
    let original = fs::read(&ctx.pdf_path)?;
    let job = ctx.create_pinned_job(JobStatus::Queued)?;
    let snapshot_path = job.snapshot_path.clone().context("job must be pinned")?;
    mutate_source_and_config(&mut ctx)?;

    worker::submit_job(&ctx.config, &ctx.db, &job.id).await?;

    let submitted = ctx.db.get_job(&job.id)?.context("job not found")?;
    assert_eq!(submitted.status, JobStatus::Processing);
    assert!(submitted.fallback_used);
    assert_eq!(
        submitted.token,
        Some(format!("{}|{snapshot_path}", sha256_hex(&original)))
    );
    assert_eq!(submitted.pdf_hash, sha256_hex(&original));
    Ok(())
}

#[tokio::test]
async fn legacy_job_backfills_snapshot_when_source_still_matches() -> Result<()> {
    let state = Arc::new(MockState::default());
    let server = MockServer::start(state.clone()).await?;
    enqueue_successful_submit(&state, &server.base_url, "tok-backfilled");

    let ctx = TestContext::new(server.base_url.clone())?;
    let original = fs::read(&ctx.pdf_path)?;
    let job = ctx.create_job(JobStatus::Queued)?;
    assert_eq!(job.snapshot_path, None, "helper creates a pre-snapshot job");

    worker::submit_job(&ctx.config, &ctx.db, &job.id).await?;

    assert_eq!(state.s3_uploads(), vec![original.clone()]);
    let submitted = ctx.db.get_job(&job.id)?.context("job not found")?;
    assert_eq!(submitted.status, JobStatus::Processing);
    let snapshot_path = submitted
        .snapshot_path
        .context("snapshot must be backfilled")?;
    assert_eq!(fs::read(snapshot_path)?, original);
    assert!(job_event_types(&ctx)?.contains(&"snapshot_backfilled".to_string()));
    Ok(())
}

#[tokio::test]
async fn legacy_job_with_edited_source_is_blocked_until_recovered() -> Result<()> {
    let state = Arc::new(MockState::default());
    let server = MockServer::start(state.clone()).await?;

    let ctx = TestContext::new(server.base_url.clone())?;
    let original = fs::read(&ctx.pdf_path)?;
    let job = ctx.create_job(JobStatus::Queued)?;
    fs::write(&ctx.pdf_path, EDITED_PDF)?;

    worker::submit_job(&ctx.config, &ctx.db, &job.id).await?;

    assert_eq!(
        state.call_count("get_upload"),
        0,
        "nothing may be submitted"
    );
    let blocked = ctx.db.get_job(&job.id)?.context("job not found")?;
    assert_eq!(blocked.status, JobStatus::FailedNeedsManual);
    assert_eq!(blocked.snapshot_path, None);
    let error = blocked
        .last_error
        .context("block reason must be recorded")?;
    assert!(error.contains(&job.pdf_hash), "{error}");
    assert!(error.contains(&sha256_hex(EDITED_PDF)), "{error}");
    assert!(
        error.contains(&format!("reviewloop retry --job-id {} --force", job.id)),
        "{error}"
    );
    assert!(
        error.contains("reviewloop submit --paper-id main"),
        "{error}"
    );
    assert!(job_event_types(&ctx)?.contains(&"submit_blocked_input_mismatch".to_string()));

    // Recovery: restore the pinned version, then retry.
    fs::write(&ctx.pdf_path, &original)?;
    ctx.db.update_job_state_unchecked(
        &job.id,
        JobStatus::Queued,
        Some(0),
        Some(None),
        Some(None),
    )?;
    enqueue_successful_submit(&state, &server.base_url, "tok-recovered");
    worker::submit_job(&ctx.config, &ctx.db, &job.id).await?;

    assert_eq!(state.s3_uploads(), vec![original]);
    let recovered = ctx.db.get_job(&job.id)?.context("job not found")?;
    assert_eq!(recovered.status, JobStatus::Processing);
    assert!(recovered.snapshot_path.is_some());
    Ok(())
}

#[tokio::test]
async fn legacy_job_with_deleted_source_is_blocked() -> Result<()> {
    let state = Arc::new(MockState::default());
    let server = MockServer::start(state.clone()).await?;

    let ctx = TestContext::new(server.base_url.clone())?;
    let job = ctx.create_job(JobStatus::Queued)?;
    fs::remove_file(&ctx.pdf_path)?;

    worker::submit_job(&ctx.config, &ctx.db, &job.id).await?;

    assert_eq!(state.call_count("get_upload"), 0);
    let blocked = ctx.db.get_job(&job.id)?.context("job not found")?;
    assert_eq!(blocked.status, JobStatus::FailedNeedsManual);
    let error = blocked.last_error.unwrap_or_default();
    assert!(error.contains("no longer exists"), "{error}");
    Ok(())
}

#[tokio::test]
async fn failed_snapshot_write_leaves_no_job() -> Result<()> {
    let state = Arc::new(MockState::default());
    let server = MockServer::start(state.clone()).await?;

    let mut ctx = TestContext::new(server.base_url.clone())?;
    ctx.config.trigger.pdf.enabled = true;
    ctx.config.trigger.pdf.auto_submit_on_change = true;
    let snapshots = ctx.config.state_dir().join("snapshots");
    fs::write(&snapshots, b"not a directory")?;

    assert!(reviewloop::trigger::run_pdf_trigger(&ctx.config, &ctx.db).is_err());
    assert!(
        ctx.db
            .list_status_views(&ctx.config.project_id, None)?
            .is_empty(),
        "no job may exist without its snapshot"
    );

    fs::remove_file(&snapshots)?;
    reviewloop::trigger::run_pdf_trigger(&ctx.config, &ctx.db)?;
    let job = ctx
        .db
        .find_latest_open_job_for_paper(&ctx.config.project_id, "main")?
        .context("trigger should enqueue once snapshots are writable")?;
    let snapshot_path = job.snapshot_path.context("job must be pinned")?;
    assert_eq!(sha256_file(Path::new(&snapshot_path))?, job.pdf_hash);
    Ok(())
}

#[tokio::test]
async fn retention_keeps_referenced_snapshots_and_removes_orphans() -> Result<()> {
    let ctx = TestContext::new("http://127.0.0.1:9".to_string())?;
    let active = ctx.create_pinned_job(JobStatus::Queued)?;
    let active_snapshot = PathBuf::from(active.snapshot_path.clone().context("pinned")?);

    let orphan_source = ctx.pdf_path.with_file_name("orphan.pdf");
    fs::write(&orphan_source, b"%PDF-1.4\n%never enqueued\n%%EOF\n")?;
    let orphan = prepare_input(&ctx.config.state_dir(), &orphan_source)?;

    let two_hours_ago = std::time::SystemTime::now() - std::time::Duration::from_secs(7200);
    for path in [
        &active_snapshot,
        &orphan.snapshot_path,
        &active_snapshot.parent().unwrap().to_path_buf(),
        &orphan.snapshot_path.parent().unwrap().to_path_buf(),
    ] {
        fs::File::open(path)?.set_modified(two_hours_ago)?;
    }

    worker::prune_retention(&ctx.config, &ctx.db, None)?;

    assert!(
        active_snapshot.exists(),
        "active job's snapshot must survive"
    );
    assert!(
        !orphan.snapshot_path.exists(),
        "unreferenced snapshot must go"
    );
    // Retention events are global (not tied to a project or job).
    let pruned = ctx
        .db
        .most_recent_event_of_type("", "retention_pruned")?
        .context("retention_pruned event")?;
    assert_eq!(pruned.payload["snapshots"], json!(1));
    Ok(())
}
