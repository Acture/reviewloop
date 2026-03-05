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
    model::{Job, JobStatus, NewJob},
    util::sha256_file,
    worker,
};
use serde_json::json;
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
}

impl MockReply {
    fn json(status: StatusCode, value: serde_json::Value) -> Self {
        Self {
            status,
            body: value.to_string(),
            content_type: "application/json",
        }
    }

    fn text(status: StatusCode, body: impl Into<String>) -> Self {
        Self {
            status,
            body: body.into(),
            content_type: "text/plain",
        }
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
    let _ = to_bytes(req.into_body(), usize::MAX).await;
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

        let mut config = Config::default();
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
        }];

        let db = Db::new(Path::new(&config.core.state_dir));
        db.init_schema()?;

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
            paper_id: "main".to_string(),
            backend: "stanford".to_string(),
            pdf_path: self.pdf_path.to_string_lossy().to_string(),
            pdf_hash,
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

#[tokio::test]
async fn integration_rate_limit_and_server_error_apply_cooldown() -> Result<()> {
    let state = Arc::new(MockState::default());
    let server = MockServer::start(state.clone()).await?;

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
    assert_cooldown_minutes(queued.next_poll_at.context("missing cooldown")?, 29, 31);

    state.enqueue_review(
        "tok-500",
        MockReply::text(StatusCode::INTERNAL_SERVER_ERROR, "backend overloaded"),
    );

    let poll_job = ctx.create_processing_job("tok-500")?;
    worker::poll_job(&ctx.config, &ctx.db, &poll_job).await?;

    let retrying = ctx.db.get_job(&poll_job.id)?.context("poll job missing")?;
    assert_eq!(retrying.status, JobStatus::Processing);
    assert_eq!(retrying.attempt, 1);
    assert_cooldown_minutes(retrying.next_poll_at.context("missing cooldown")?, 29, 31);
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
