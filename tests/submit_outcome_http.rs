//! OSS-337 acceptance (5), end to end through the real Stanford backend and fallback
//! runner against a mock provider: a submission whose outcome is unknown is parked as
//! SUBMITTED/UNCERTAIN and is never resent or handed to the fallback, while definitive
//! rejections, rate limits and late receipts keep their documented behavior.

mod common;

use anyhow::{Context, Result};
use axum::{
    Router,
    body::to_bytes,
    extract::{Request, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::{MethodRouter, post},
};
use chrono::{DateTime, Duration, Utc};
use common::{Ctx, EMAIL, PROJECT, assert_no_lease, event_types, node_available};
use reviewloop::{
    backend::stanford::{StanfordBackend, StepTimeouts},
    db::{CancelOutcome, ClaimTiming, LeaseRecovery},
    http::{Redirects, build_client},
    model::{EventRecord, Job, JobStatus, SubmitStage, WorkKind},
    worker::{self, Attempt},
};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    fs,
    future::Future,
    net::SocketAddr,
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    pin::Pin,
    process::Command,
    sync::{Arc, Mutex},
    time::Duration as StdDuration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Notify,
    task::{JoinHandle, JoinSet},
};

const GET_UPLOAD: &str = "get_upload";
const S3: &str = "s3_upload";
const CONFIRM: &str = "confirm_upload";
/// Upper bound on any wait on the worker or the mock; the worker's own dispatch timeout
/// is 20 minutes, so hitting this means a test is exercising the wrong path.
const GUARD: StdDuration = StdDuration::from_secs(60);

// ---------------------------------------------------------------------------------------
// Mock provider
// ---------------------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct Reply {
    status: StatusCode,
    body: String,
    content_type: &'static str,
    headers: Vec<(&'static str, String)>,
}

impl Reply {
    fn json(status: StatusCode, value: Value) -> Self {
        Self {
            status,
            body: value.to_string(),
            content_type: "application/json",
            headers: vec![],
        }
    }

    fn text(status: StatusCode, body: &str) -> Self {
        Self {
            status,
            body: body.to_string(),
            content_type: "text/plain",
            headers: vec![],
        }
    }

    fn header(mut self, name: &'static str, value: &str) -> Self {
        self.headers.push((name, value.to_string()));
        self
    }
}

impl IntoResponse for Reply {
    fn into_response(self) -> Response {
        let mut response = (self.status, self.body).into_response();
        let headers = response.headers_mut();
        headers.insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static(self.content_type),
        );
        for (name, value) in self.headers {
            headers.insert(
                header::HeaderName::from_static(name),
                header::HeaderValue::from_str(&value).expect("valid mock header value"),
            );
        }
        response
    }
}

/// What a route does with the next request it receives.
enum Step {
    Reply(Reply),
    /// Signal `entered`, wait for `release`, then reply: the request has reached the
    /// provider and the test acts while the worker awaits the receipt.
    Gate(Reply),
    /// Signal `entered` and never answer.
    Hang,
}

#[derive(Default)]
struct MockState {
    steps: Mutex<HashMap<&'static str, VecDeque<Step>>>,
    calls: Mutex<HashMap<&'static str, usize>>,
    bodies: Mutex<HashMap<&'static str, Vec<Vec<u8>>>>,
    entered: Notify,
    release: Notify,
}

impl MockState {
    fn push(&self, route: &'static str, step: Step) {
        self.steps
            .lock()
            .unwrap()
            .entry(route)
            .or_default()
            .push_back(step);
    }

    fn calls(&self, route: &'static str) -> usize {
        self.calls.lock().unwrap().get(route).copied().unwrap_or(0)
    }

    /// Raw request bodies `route` received, in order.
    fn bodies(&self, route: &'static str) -> Vec<Vec<u8>> {
        self.bodies
            .lock()
            .unwrap()
            .get(route)
            .cloned()
            .unwrap_or_default()
    }

    async fn serve(&self, route: &'static str, req: Request) -> Response {
        *self.calls.lock().unwrap().entry(route).or_insert(0) += 1;
        // Read the whole body first, so a gated request has provably been delivered.
        let body = to_bytes(req.into_body(), usize::MAX)
            .await
            .unwrap_or_default();
        self.bodies
            .lock()
            .unwrap()
            .entry(route)
            .or_default()
            .push(body.to_vec());
        let step = self
            .steps
            .lock()
            .unwrap()
            .get_mut(route)
            .and_then(VecDeque::pop_front);
        match step {
            Some(Step::Reply(reply)) => reply.into_response(),
            Some(Step::Gate(reply)) => {
                // notify_one keeps a permit, so the test cannot miss the signal.
                self.entered.notify_one();
                self.release.notified().await;
                reply.into_response()
            }
            Some(Step::Hang) => {
                self.entered.notify_one();
                std::future::pending().await
            }
            None => Reply::json(
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({ "detail": format!("mock {route} queue is empty") }),
            )
            .into_response(),
        }
    }
}

fn route(name: &'static str) -> MethodRouter<Arc<MockState>> {
    post(
        move |State(state): State<Arc<MockState>>, req: Request| async move {
            state.serve(name, req).await
        },
    )
}

/// axum app behind a TCP relay that owns every client connection. `axum::serve` runs
/// each connection in its own task, so aborting it would not drop an in-flight request;
/// aborting the relay drops both ends of every connection and stops accepting new ones, as
/// if the provider vanished mid-request.
struct MockServer {
    /// Provider API (`/api/*`), reached through the relay.
    base_url: String,
    /// The presigned S3 upload target, reached directly: a different host in production.
    s3_url: String,
    state: Arc<MockState>,
    app: JoinHandle<()>,
    relay: Mutex<Option<JoinHandle<()>>>,
}

impl MockServer {
    async fn start() -> Result<Self> {
        let state = Arc::new(MockState::default());
        let app = Router::new()
            .route("/api/get-upload-url", route(GET_UPLOAD))
            .route("/api/confirm-upload", route(CONFIRM))
            .route("/s3/upload", route(S3))
            .with_state(state.clone());
        let app_listener = TcpListener::bind("127.0.0.1:0").await?;
        let app_addr = app_listener.local_addr()?;
        let app = tokio::spawn(async move {
            let _ = axum::serve(app_listener, app).await;
        });
        let front = TcpListener::bind("127.0.0.1:0").await?;
        let base_url = format!("http://{}", front.local_addr()?);
        let relay = tokio::spawn(relay(front, app_addr));
        Ok(Self {
            base_url,
            s3_url: format!("http://{app_addr}/s3/upload"),
            state,
            app,
            relay: Mutex::new(Some(relay)),
        })
    }

    /// Drop every open provider connection without a response and stop accepting new ones.
    async fn cut_connections(&self) {
        let relay = self.relay.lock().unwrap().take();
        if let Some(relay) = relay {
            relay.abort();
            // Once the task is gone the listener is closed; its pipes are aborted with it.
            let _ = relay.await;
        }
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        if let Some(relay) = self.relay.lock().unwrap().take() {
            relay.abort();
        }
        self.app.abort();
    }
}

async fn relay(front: TcpListener, upstream: SocketAddr) {
    // Dropping the set (when this task is aborted) aborts every pipe and closes its sockets.
    let mut pipes = JoinSet::new();
    while let Ok((mut client, _)) = front.accept().await {
        pipes.spawn(async move {
            if let Ok(mut server) = TcpStream::connect(upstream).await {
                let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
            }
        });
    }
}

// ---------------------------------------------------------------------------------------
// Test context
// ---------------------------------------------------------------------------------------

/// The shared test project with its provider pointed at a [`MockServer`].
struct TestContext {
    ctx: Ctx,
    server: MockServer,
}

impl Deref for TestContext {
    type Target = Ctx;

    fn deref(&self) -> &Ctx {
        &self.ctx
    }
}

impl DerefMut for TestContext {
    fn deref_mut(&mut self) -> &mut Ctx {
        &mut self.ctx
    }
}

impl TestContext {
    /// Fallback disabled; tests that exercise it opt in with [`Self::arm_fallback`].
    async fn start() -> Result<Self> {
        let server = MockServer::start().await?;
        let mut ctx = Ctx::new()?;
        ctx.config.providers.stanford.base_url = server.base_url.clone();
        Ok(Self { ctx, server })
    }

    fn mock(&self) -> &MockState {
        &self.server.state
    }

    /// `[get-upload-url, S3 upload, confirm-upload]` request counts.
    fn calls(&self) -> [usize; 3] {
        let mock = self.mock();
        [mock.calls(GET_UPLOAD), mock.calls(S3), mock.calls(CONFIRM)]
    }

    /// Script the steps before confirm-upload to succeed.
    fn upload_succeeds(&self) {
        self.mock()
            .push(GET_UPLOAD, Step::Reply(self.upload_url_reply()));
        self.mock()
            .push(S3, Step::Reply(Reply::text(StatusCode::NO_CONTENT, "")));
    }

    fn upload_url_reply(&self) -> Reply {
        Reply::json(
            StatusCode::OK,
            json!({
                "success": true,
                "presigned_url": self.server.s3_url,
                "s3_key": "uploads/paper.pdf",
                "presigned_fields": { "key": "uploads/paper.pdf", "policy": "p" }
            }),
        )
    }

    fn confirm(&self, step: Step) {
        self.mock().push(CONFIRM, step);
    }

    /// Enable the fallback with a node script that appends its argv to a log, then runs
    /// `behavior`. Returns the log path.
    fn arm_fallback(&mut self, behavior: &str) -> Result<PathBuf> {
        let log = self.tmp.path().join("fallback-invocations.log");
        let log_literal = serde_json::to_string(&log.to_string_lossy())?;
        self.arm_fallback_script(&format!(
            "require('fs').appendFileSync({log_literal}, JSON.stringify(process.argv.slice(2)) + '\\n');\n{behavior}\n"
        ))?;
        Ok(log)
    }

    /// Submit through the real Stanford backend, as `reviewloop submit` does.
    async fn submit(&self, job: &Job) -> Result<Attempt> {
        tokio::time::timeout(GUARD, worker::submit_job(&self.config, &self.db, &job.id))
            .await
            .context("submit did not finish")?
    }

    /// Run daemon ticks and check they leave the job, its events and the provider alone.
    async fn assert_ticks_leave_alone(&self, job: &Job, ticks: usize) -> Result<()> {
        let before = serde_json::to_value(self.job(&job.id)?)?;
        let events_before = self.events(&job.id)?.len();
        let calls_before = self.calls();
        for _ in 0..ticks {
            worker::run_tick(&self.config, &self.db).await?;
        }
        assert_eq!(serde_json::to_value(self.job(&job.id)?)?, before);
        assert_eq!(self.events(&job.id)?.len(), events_before);
        assert_eq!(self.calls(), calls_before, "a tick contacted the provider");
        Ok(())
    }
}

// ---------------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------------

const FALLBACK_SUCCEEDS: &str =
    r#"console.log(JSON.stringify({ success: true, token: "fallback-token" }));"#;

fn fallback_runs(log: &Path) -> Result<Vec<Vec<String>>> {
    if !log.exists() {
        return Ok(vec![]);
    }
    fs::read_to_string(log)?
        .lines()
        .map(|line| serde_json::from_str(line).map_err(Into::into))
        .collect()
}

fn arg_after<'a>(argv: &'a [String], flag: &str) -> Option<&'a str> {
    let at = argv.iter().position(|arg| arg == flag)?;
    argv.get(at + 1).map(String::as_str)
}

/// Drive `worker` until a gated request reaches the mock provider. The worker stays alive.
async fn until_entered<F>(worker: Pin<&mut F>, mock: &MockState) -> Result<()>
where
    F: Future,
    F::Output: Debug,
{
    tokio::time::timeout(GUARD, async {
        tokio::select! {
            out = worker => anyhow::bail!("worker finished before the gated request arrived: {out:?}"),
            () = mock.entered.notified() => Ok(()),
        }
    })
    .await
    .context("the gated request never reached the mock provider")?
}

async fn finish<F>(worker: Pin<&mut F>) -> Result<Attempt>
where
    F: Future<Output = Result<Attempt>>,
{
    tokio::time::timeout(GUARD, worker)
        .await
        .context("worker did not finish")?
}

fn assert_contains(haystack: Option<&str>, needle: &str) {
    let haystack = haystack.unwrap_or_default();
    assert!(
        haystack.contains(needle),
        "expected {needle:?} in {haystack:?}"
    );
}

/// Parked as SUBMITTED/UNCERTAIN by the worker after one dispatch through `channel`.
fn assert_uncertain(job: &Job, channel: &str, details: &[&str]) {
    assert_eq!(job.status, JobStatus::Submitted);
    assert_eq!(job.submit_stage, Some(SubmitStage::Uncertain));
    assert_eq!(job.attempt, 1);
    assert_eq!(job.token, None);
    assert_eq!(job.next_poll_at, None);
    assert_no_lease(job);
    assert_contains(
        job.last_error.as_deref(),
        &format!("submission outcome unknown ({channel} channel)"),
    );
    assert_contains(job.last_error.as_deref(), "import-token");
    for detail in details {
        assert_contains(job.last_error.as_deref(), detail);
    }
}

fn assert_dispatched(event: &EventRecord, channel: &str) {
    assert_eq!(event.event_type, "submit_dispatched");
    assert_eq!(event.payload["channel"], channel);
}

fn assert_outcome_unknown_event(event: &EventRecord, channel: &str, detail: &str) {
    assert_eq!(event.event_type, "submit_outcome_unknown");
    assert_eq!(event.payload["source"], "dispatch_error");
    assert_eq!(event.payload["channel"], channel);
    assert_contains(event.payload["error"].as_str(), detail);
}

fn assert_minutes_from_now(at: Option<DateTime<Utc>>, min: i64, max: i64) {
    let minutes = (at.expect("next_poll_at set") - Utc::now()).num_minutes();
    assert!(
        (min..=max).contains(&minutes),
        "expected next_poll_at in [{min}, {max}] minutes, got {minutes}"
    );
}

// ---------------------------------------------------------------------------------------
// confirm-upload outcome unknown: park, never fall back, never resend
// ---------------------------------------------------------------------------------------

/// The fallback is armed with a script that would succeed, so routing an unknown outcome
/// to it would show up as PROCESSING even where node is missing.
async fn assert_confirm_reply_parks_uncertain(confirm: Reply, detail: &str) -> Result<()> {
    let mut ctx = TestContext::start().await?;
    let log = ctx.arm_fallback(FALLBACK_SUCCEEDS)?;
    ctx.upload_succeeds();
    ctx.confirm(Step::Reply(confirm));
    let job = ctx.create_queued_job()?;

    assert_eq!(ctx.submit(&job).await?, Attempt::Ran);

    let parked = ctx.job(&job.id)?;
    assert_uncertain(&parked, "primary", &[detail]);
    assert!(!parked.fallback_used);
    let events = ctx.events(&job.id)?;
    assert_eq!(
        event_types(&events),
        ["submit_dispatched", "submit_outcome_unknown"]
    );
    assert_dispatched(&events[0], "primary");
    assert_outcome_unknown_event(&events[1], "primary", detail);
    assert_eq!(ctx.calls(), [1, 1, 1]);
    assert!(fallback_runs(&log)?.is_empty(), "fallback ran");

    ctx.assert_ticks_leave_alone(&job, 2).await?;
    assert!(fallback_runs(&log)?.is_empty(), "fallback ran on a tick");
    Ok(())
}

#[tokio::test]
async fn confirm_500_parks_uncertain_without_fallback_and_ticks_never_resend() -> Result<()> {
    assert_confirm_reply_parks_uncertain(
        Reply::json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({ "detail": "upstream exploded" }),
        ),
        "confirm-upload returned 500 Internal Server Error",
    )
    .await
}

#[tokio::test]
async fn confirm_200_unparseable_receipt_parks_uncertain() -> Result<()> {
    assert_confirm_reply_parks_uncertain(
        Reply::text(StatusCode::OK, "<html>gateway hiccup</html>"),
        "invalid confirm-upload receipt",
    )
    .await
}

#[tokio::test]
async fn confirm_200_success_without_token_parks_uncertain() -> Result<()> {
    assert_confirm_reply_parks_uncertain(
        Reply::json(StatusCode::OK, json!({ "success": true })),
        "confirm-upload succeeded without a token",
    )
    .await
}

#[tokio::test]
async fn confirm_response_lost_parks_uncertain() -> Result<()> {
    let mut ctx = TestContext::start().await?;
    let log = ctx.arm_fallback(FALLBACK_SUCCEEDS)?;
    ctx.upload_succeeds();
    ctx.confirm(Step::Hang);
    let job = ctx.create_queued_job()?;

    let worker = worker::submit_job(&ctx.config, &ctx.db, &job.id);
    tokio::pin!(worker);
    until_entered(worker.as_mut(), ctx.mock()).await?;
    // The provider holds the request; the connection dies before any response.
    ctx.server.cut_connections().await;
    assert_eq!(finish(worker).await?, Attempt::Ran);

    let parked = ctx.job(&job.id)?;
    // "got no response" proves the send-error branch, not the 20-minute dispatch timeout.
    assert_uncertain(&parked, "primary", &["confirm-upload got no response"]);
    assert!(!parked.fallback_used);
    let events = ctx.events(&job.id)?;
    assert_eq!(
        event_types(&events),
        ["submit_dispatched", "submit_outcome_unknown"]
    );
    assert_outcome_unknown_event(&events[1], "primary", "confirm-upload got no response");
    assert_eq!(ctx.calls(), [1, 1, 1]);
    assert!(fallback_runs(&log)?.is_empty(), "fallback ran");

    ctx.assert_ticks_leave_alone(&job, 2).await?;
    Ok(())
}

/// The worker dies while confirm-upload is in flight. Its live lease keeps every tick off
/// the job; once the lease lapses, recovery parks it as UNCERTAIN instead of requeueing.
#[tokio::test]
async fn worker_crash_during_confirm_recovers_as_uncertain_after_lease_expiry() -> Result<()> {
    let ctx = TestContext::start().await?;
    ctx.upload_succeeds();
    ctx.confirm(Step::Hang);
    let job = ctx.create_queued_job()?;

    {
        let worker = worker::submit_job(&ctx.config, &ctx.db, &job.id);
        tokio::pin!(worker);
        until_entered(worker.as_mut(), ctx.mock()).await?;
    } // the worker future is dropped here: the crash

    let in_flight = ctx.job(&job.id)?;
    assert_eq!(in_flight.status, JobStatus::Submitted);
    assert_eq!(in_flight.submit_stage, Some(SubmitStage::Dispatched));
    let owner = in_flight.lease_owner.clone().context("dispatch lease")?;
    assert_minutes_from_now(in_flight.lease_expires_at, 29, 30);

    ctx.assert_ticks_leave_alone(&job, 1).await?;

    let report = ctx
        .db
        .recover_expired_leases(PROJECT, Utc::now() + Duration::minutes(31))?;
    assert_eq!(
        report,
        LeaseRecovery {
            released_claims: 0,
            uncertain_submits: 1
        }
    );
    let parked = ctx.job(&job.id)?;
    assert_eq!(parked.status, JobStatus::Submitted);
    assert_eq!(parked.submit_stage, Some(SubmitStage::Uncertain));
    assert_eq!(parked.token, None);
    assert_no_lease(&parked);
    assert_contains(
        parked.last_error.as_deref(),
        "lost its lease after dispatching",
    );
    assert_contains(parked.last_error.as_deref(), "import-token");
    let events = ctx.events(&job.id)?;
    assert_eq!(
        event_types(&events),
        ["submit_dispatched", "submit_outcome_unknown"]
    );
    assert_eq!(events[1].payload["source"], "lease_expired");
    assert_eq!(events[1].payload["previous_owner"], owner.as_str());

    ctx.assert_ticks_leave_alone(&job, 2).await?;
    assert_eq!(ctx.calls(), [1, 1, 1]);
    Ok(())
}

// ---------------------------------------------------------------------------------------
// Receipts that arrive after the worker lost the job are kept for recovery
// ---------------------------------------------------------------------------------------

#[tokio::test]
async fn late_receipt_after_cancel_keeps_cancel_and_stores_token() -> Result<()> {
    let ctx = TestContext::start().await?;
    ctx.upload_succeeds();
    ctx.confirm(Step::Gate(Reply::json(
        StatusCode::OK,
        json!({ "success": true, "token": "tok-late" }),
    )));
    let job = ctx.create_queued_job()?;

    let worker = worker::submit_job(&ctx.config, &ctx.db, &job.id);
    tokio::pin!(worker);
    until_entered(worker.as_mut(), ctx.mock()).await?;
    let cancel = ctx
        .db
        .cancel_job(&job.id, Some("changed my mind"), Utc::now())?;
    assert_eq!(
        cancel,
        CancelOutcome::Cancelled {
            previous_status: JobStatus::Submitted,
            previous_stage: Some(SubmitStage::Dispatched),
            lease_was_active: true,
        }
    );
    ctx.mock().release.notify_one();
    assert_eq!(finish(worker).await?, Attempt::Ran);

    let cancelled = ctx.job(&job.id)?;
    assert_eq!(cancelled.status, JobStatus::Failed);
    assert_eq!(
        cancelled.last_error.as_deref(),
        Some("cancelled by user: changed my mind")
    );
    assert_eq!(cancelled.token.as_deref(), Some("tok-late"));
    assert_eq!(cancelled.submit_stage, None);
    assert_no_lease(&cancelled);
    let events = ctx.events(&job.id)?;
    assert_eq!(
        event_types(&events),
        [
            "submit_dispatched",
            "cancelled",
            "submit_receipt_after_lease_lost"
        ]
    );
    assert_eq!(events[1].payload["lease_was_active"], true);
    assert_eq!(events[1].payload["previous_submit_stage"], "DISPATCHED");
    let receipt = &events[2].payload;
    assert_eq!(receipt["token"], "tok-late");
    assert_eq!(receipt["stored"], true);
    assert_eq!(receipt["status"], "FAILED");
    assert_eq!(receipt["channel"], "primary");

    ctx.assert_ticks_leave_alone(&job, 1).await?;
    assert_eq!(ctx.calls(), [1, 1, 1]);
    Ok(())
}

#[tokio::test]
async fn late_receipt_after_lease_expiry_is_stored_without_resuming() -> Result<()> {
    let ctx = TestContext::start().await?;
    ctx.upload_succeeds();
    ctx.confirm(Step::Gate(Reply::json(
        StatusCode::OK,
        json!({ "success": true, "token": "tok-late" }),
    )));
    let job = ctx.create_queued_job()?;

    let worker = worker::submit_job(&ctx.config, &ctx.db, &job.id);
    tokio::pin!(worker);
    until_entered(worker.as_mut(), ctx.mock()).await?;
    let report = ctx
        .db
        .recover_expired_leases(PROJECT, Utc::now() + Duration::minutes(31))?;
    assert_eq!(report.uncertain_submits, 1);
    ctx.mock().release.notify_one();
    assert_eq!(finish(worker).await?, Attempt::Ran);

    let parked = ctx.job(&job.id)?;
    assert_eq!(parked.status, JobStatus::Submitted);
    assert_eq!(parked.submit_stage, Some(SubmitStage::Uncertain));
    assert_eq!(parked.token.as_deref(), Some("tok-late"));
    assert_eq!(parked.attempt, 0);
    assert_no_lease(&parked);
    assert_contains(
        parked.last_error.as_deref(),
        "receipt arrived after the worker lost its lease",
    );
    assert_contains(
        parked.last_error.as_deref(),
        &format!("reviewloop retry --job-id {}", job.id),
    );
    let events = ctx.events(&job.id)?;
    assert_eq!(
        event_types(&events),
        [
            "submit_dispatched",
            "submit_outcome_unknown",
            "submit_receipt_after_lease_lost"
        ]
    );
    assert_eq!(events[1].payload["source"], "lease_expired");
    assert_eq!(events[2].payload["stored"], true);
    assert_eq!(events[2].payload["status"], "SUBMITTED");

    ctx.assert_ticks_leave_alone(&job, 2).await?;
    assert_eq!(ctx.calls(), [1, 1, 1]);
    Ok(())
}

// ---------------------------------------------------------------------------------------
// Definitive rejections keep their old behavior
// ---------------------------------------------------------------------------------------

async fn assert_definitive_failure(ctx: &TestContext, job: &Job, reason: &str) -> Result<()> {
    assert_eq!(ctx.submit(job).await?, Attempt::Ran);
    let failed = ctx.job(&job.id)?;
    assert_eq!(failed.status, JobStatus::Failed);
    assert_eq!(failed.attempt, 1);
    assert_eq!(failed.submit_stage, None);
    assert_eq!(failed.token, None);
    assert_eq!(failed.next_poll_at, None);
    assert!(!failed.fallback_used);
    assert_no_lease(&failed);
    assert_contains(failed.last_error.as_deref(), reason);
    let events = ctx.events(&job.id)?;
    assert_eq!(event_types(&events), ["submit_dispatched", "submit_failed"]);
    assert_eq!(
        events[1].payload["reason"].as_str(),
        failed.last_error.as_deref()
    );
    Ok(())
}

/// The primary was rejected, so the armed fallback runs and its receipt is accepted.
async fn assert_handed_to_fallback(
    ctx: &TestContext,
    job: &Job,
    log: &Path,
    calls: [usize; 3],
) -> Result<()> {
    assert_eq!(ctx.submit(job).await?, Attempt::Ran);
    let after = ctx.job(&job.id)?;
    assert!(after.fallback_used);
    assert_eq!(after.submit_stage, None);
    assert_no_lease(&after);
    let events = ctx.events(&job.id)?;
    assert_dispatched(&events[0], "primary");
    assert_dispatched(&events[1], "fallback");
    assert_eq!(ctx.calls(), calls);

    if !node_available() {
        eprintln!("node unavailable: checking the spawn-failure outcome only");
        assert_eq!(after.status, JobStatus::FailedNeedsManual);
        assert_contains(
            after.last_error.as_deref(),
            "failed to execute node fallback",
        );
        return Ok(());
    }
    assert_eq!(after.status, JobStatus::Processing);
    assert_eq!(after.token.as_deref(), Some("fallback-token"));
    assert_eq!(after.attempt, 0);
    assert_eq!(after.last_error, None);
    assert_minutes_from_now(after.next_poll_at, 9, 10);
    assert_eq!(event_types(&events)[2..], ["submitted_via_fallback"]);
    assert_eq!(events[2].payload["token"], "fallback-token");
    assert_eq!(events[2].payload["channel"], "fallback");
    let runs = fallback_runs(log)?;
    assert_eq!(runs.len(), 1, "fallback runs: {runs:?}");
    // The fallback uploads the job's pinned snapshot, never the live source file.
    let snapshot = after.snapshot_path.as_deref().expect("pinned snapshot");
    assert_eq!(arg_after(&runs[0], "--pdf"), Some(snapshot));
    assert_eq!(arg_after(&runs[0], "--email"), Some(EMAIL));
    assert_eq!(
        arg_after(&runs[0], "--base-url"),
        Some(ctx.server.base_url.as_str())
    );
    Ok(())
}

#[tokio::test]
async fn get_upload_500_is_definitive_and_hands_off_to_fallback() -> Result<()> {
    let mut ctx = TestContext::start().await?;
    let log = ctx.arm_fallback(FALLBACK_SUCCEEDS)?;
    ctx.mock().push(
        GET_UPLOAD,
        Step::Reply(Reply::json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({ "detail": "forced primary failure" }),
        )),
    );
    let job = ctx.create_queued_job()?;
    assert_handed_to_fallback(&ctx, &job, &log, [1, 0, 0]).await
}

fn confirm_success_false() -> Reply {
    Reply::json(
        StatusCode::OK,
        json!({ "success": false, "detail": "paper rejected by provider" }),
    )
}

fn confirm_400() -> Reply {
    Reply::json(
        StatusCode::BAD_REQUEST,
        json!({ "detail": "malformed form" }),
    )
}

#[tokio::test]
async fn confirm_success_false_fails_without_fallback() -> Result<()> {
    let ctx = TestContext::start().await?;
    ctx.upload_succeeds();
    ctx.confirm(Step::Reply(confirm_success_false()));
    let job = ctx.create_queued_job()?;
    assert_definitive_failure(&ctx, &job, "schema error: paper rejected by provider").await?;
    assert_eq!(ctx.calls(), [1, 1, 1]);
    Ok(())
}

#[tokio::test]
async fn confirm_success_false_hands_off_to_fallback() -> Result<()> {
    let mut ctx = TestContext::start().await?;
    let log = ctx.arm_fallback(FALLBACK_SUCCEEDS)?;
    ctx.upload_succeeds();
    ctx.confirm(Step::Reply(confirm_success_false()));
    let job = ctx.create_queued_job()?;
    assert_handed_to_fallback(&ctx, &job, &log, [1, 1, 1]).await
}

#[tokio::test]
async fn confirm_400_fails_without_fallback() -> Result<()> {
    let ctx = TestContext::start().await?;
    ctx.upload_succeeds();
    ctx.confirm(Step::Reply(confirm_400()));
    let job = ctx.create_queued_job()?;
    assert_definitive_failure(
        &ctx,
        &job,
        "schema error: confirm-upload failed (400 Bad Request)",
    )
    .await?;
    assert_eq!(ctx.calls(), [1, 1, 1]);
    Ok(())
}

#[tokio::test]
async fn confirm_400_hands_off_to_fallback() -> Result<()> {
    let mut ctx = TestContext::start().await?;
    let log = ctx.arm_fallback(FALLBACK_SUCCEEDS)?;
    ctx.upload_succeeds();
    ctx.confirm(Step::Reply(confirm_400()));
    let job = ctx.create_queued_job()?;
    assert_handed_to_fallback(&ctx, &job, &log, [1, 1, 1]).await
}

// ---------------------------------------------------------------------------------------
// Rate limits requeue with the existing cooldown
// ---------------------------------------------------------------------------------------

#[tokio::test]
async fn confirm_429_requeues_with_schedule_cooldown_and_ticks_respect_it() -> Result<()> {
    let mut ctx = TestContext::start().await?;
    // A rate limit is a definitive "not now": it must not reach the fallback either.
    let log = ctx.arm_fallback(FALLBACK_SUCCEEDS)?;
    ctx.upload_succeeds();
    ctx.confirm(Step::Reply(Reply::json(
        StatusCode::TOO_MANY_REQUESTS,
        json!({ "detail": "slow down" }),
    )));
    let job = ctx.create_queued_job()?;

    assert_eq!(ctx.submit(&job).await?, Attempt::Ran);

    let queued = ctx.job(&job.id)?;
    assert_eq!(queued.status, JobStatus::Queued);
    assert_eq!(queued.attempt, 1);
    assert_eq!(queued.submit_stage, None);
    assert_eq!(queued.token, None);
    assert!(!queued.fallback_used);
    assert_no_lease(&queued);
    assert_contains(queued.last_error.as_deref(), "slow down");
    // schedule [10, 20, 40, 60], attempt 1, jitter 0 -> 20 minutes
    assert_minutes_from_now(queued.next_poll_at, 19, 20);
    let events = ctx.events(&job.id)?;
    assert_eq!(
        event_types(&events),
        ["submit_dispatched", "submit_rate_limited"]
    );
    assert_eq!(events[1].payload["retry_after_source"], "schedule");
    assert_eq!(ctx.calls(), [1, 1, 1]);
    assert!(fallback_runs(&log)?.is_empty(), "fallback ran");

    // Ticks inside the cooldown leave it alone.
    ctx.assert_ticks_leave_alone(&job, 2).await?;
    let now = Utc::now();
    let early = ctx.db.claim_job(
        &job.id,
        WorkKind::Submit,
        ClaimTiming::WhenDue,
        now + Duration::minutes(19),
        Duration::minutes(30),
    )?;
    assert!(early.is_none(), "claimable before the cooldown ended");
    let due = ctx
        .db
        .claim_job(
            &job.id,
            WorkKind::Submit,
            ClaimTiming::WhenDue,
            now + Duration::minutes(21),
            Duration::minutes(30),
        )?
        .context("not claimable after the cooldown")?;
    assert!(ctx.db.release_lease(&due)?);

    // Once due, the daemon retries normally.
    ctx.db.update_job_state(
        &job.id,
        JobStatus::Queued,
        None,
        Some(Some(Utc::now() - Duration::seconds(1))),
        None,
    )?;
    ctx.upload_succeeds();
    ctx.confirm(Step::Reply(Reply::json(
        StatusCode::OK,
        json!({ "success": true, "token": "tok-after-cooldown" }),
    )));
    worker::run_tick(&ctx.config, &ctx.db).await?;
    let submitted = ctx.job(&job.id)?;
    assert_eq!(submitted.status, JobStatus::Processing);
    assert_eq!(submitted.token.as_deref(), Some("tok-after-cooldown"));
    assert!(!submitted.fallback_used);
    assert_eq!(ctx.calls(), [2, 2, 2]);
    assert!(fallback_runs(&log)?.is_empty(), "fallback ran");
    Ok(())
}

#[tokio::test]
async fn confirm_429_with_retry_after_uses_server_delay() -> Result<()> {
    let ctx = TestContext::start().await?;
    ctx.upload_succeeds();
    ctx.confirm(Step::Reply(
        Reply::json(
            StatusCode::TOO_MANY_REQUESTS,
            json!({ "detail": "slow down" }),
        )
        .header("retry-after", "120"),
    ));
    let job = ctx.create_queued_job()?;
    let before = Utc::now();

    assert_eq!(ctx.submit(&job).await?, Attempt::Ran);

    let queued = ctx.job(&job.id)?;
    assert_eq!(queued.status, JobStatus::Queued);
    assert_eq!(queued.attempt, 1);
    assert_no_lease(&queued);
    let delay = (queued.next_poll_at.context("next_poll_at")? - before).num_seconds();
    assert!((115..=130).contains(&delay), "expected ~120s, got {delay}s");
    let events = ctx.events(&job.id)?;
    assert_eq!(events[1].event_type, "submit_rate_limited");
    assert_eq!(events[1].payload["retry_after_source"], "server");
    Ok(())
}

// ---------------------------------------------------------------------------------------
// Fallback script contract (needs node)
// ---------------------------------------------------------------------------------------

/// Reject the primary definitively, run the fallback `behavior`, and check the common
/// hand-off. `None` when node is unavailable.
async fn run_fallback(behavior: &str) -> Result<Option<(TestContext, Job, PathBuf)>> {
    if !node_available() {
        eprintln!("skipped: node is not available");
        return Ok(None);
    }
    let mut ctx = TestContext::start().await?;
    let log = ctx.arm_fallback(behavior)?;
    ctx.mock().push(
        GET_UPLOAD,
        Step::Reply(Reply::json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({ "detail": "forced primary failure" }),
        )),
    );
    let job = ctx.create_queued_job()?;

    assert_eq!(ctx.submit(&job).await?, Attempt::Ran);

    let after = ctx.job(&job.id)?;
    assert_no_lease(&after);
    let events = ctx.events(&job.id)?;
    assert_dispatched(&events[0], "primary");
    assert_dispatched(&events[1], "fallback");
    assert_eq!(ctx.calls(), [1, 0, 0]);
    assert_eq!(fallback_runs(&log)?.len(), 1);
    Ok(Some((ctx, job, log)))
}

async fn assert_fallback_parks_uncertain(behavior: &str, detail: &str) -> Result<()> {
    let Some((ctx, job, log)) = run_fallback(behavior).await? else {
        return Ok(());
    };
    let parked = ctx.job(&job.id)?;
    assert!(parked.fallback_used, "the fallback may have submitted");
    assert_uncertain(
        &parked,
        "fallback",
        &["primary submit error: server error (500)", detail],
    );
    let events = ctx.events(&job.id)?;
    assert_eq!(event_types(&events).len(), 3);
    assert_outcome_unknown_event(&events[2], "fallback", detail);

    ctx.assert_ticks_leave_alone(&job, 2).await?;
    assert_eq!(fallback_runs(&log)?.len(), 1, "fallback ran again");
    Ok(())
}

async fn assert_fallback_fails_needs_manual(behavior: &str, detail: &str) -> Result<()> {
    let Some((ctx, job, _log)) = run_fallback(behavior).await? else {
        return Ok(());
    };
    let failed = ctx.job(&job.id)?;
    assert_eq!(failed.status, JobStatus::FailedNeedsManual);
    assert!(
        !failed.fallback_used,
        "a fallback that never reached the provider stays available for a retry"
    );
    assert_eq!(failed.attempt, 1);
    assert_eq!(failed.submit_stage, None);
    assert_eq!(failed.token, None);
    assert_contains(
        failed.last_error.as_deref(),
        "primary submit error: server error (500)",
    );
    assert_contains(
        failed.last_error.as_deref(),
        "fallback error: command error",
    );
    assert_contains(failed.last_error.as_deref(), detail);
    let events = ctx.events(&job.id)?;
    assert_eq!(event_types(&events)[2..], ["submit_failed_needs_manual"]);
    Ok(())
}

#[tokio::test]
async fn fallback_failure_after_submit_click_parks_uncertain() -> Result<()> {
    assert_fallback_parks_uncertain(
        r#"console.error(JSON.stringify({ success: false, submitted: true, error: "x" })); process.exit(1);"#,
        "fallback exited with status",
    )
    .await
}

#[tokio::test]
async fn fallback_failure_without_submitted_flag_parks_uncertain() -> Result<()> {
    assert_fallback_parks_uncertain(
        r#"console.error(JSON.stringify({ success: false, error: "x" })); process.exit(1);"#,
        "fallback exited with status",
    )
    .await
}

#[tokio::test]
async fn fallback_failure_before_submit_click_fails_needs_manual() -> Result<()> {
    assert_fallback_fails_needs_manual(
        r#"console.error(JSON.stringify({ success: false, submitted: false, error: "x" })); process.exit(1);"#,
        "fallback exited with status",
    )
    .await
}

#[tokio::test]
async fn fallback_exit_zero_with_garbage_parks_uncertain() -> Result<()> {
    assert_fallback_parks_uncertain(
        r#"console.log("definitely not a json report");"#,
        "fallback exited successfully without a JSON report",
    )
    .await
}

#[tokio::test]
async fn fallback_exit_zero_with_token_moves_to_processing() -> Result<()> {
    let Some((ctx, job, _log)) =
        run_fallback(r#"console.log(JSON.stringify({ success: true, token: "t" }));"#).await?
    else {
        return Ok(());
    };
    let submitted = ctx.job(&job.id)?;
    assert_eq!(submitted.status, JobStatus::Processing);
    assert!(submitted.fallback_used);
    assert_eq!(submitted.token.as_deref(), Some("t"));
    assert_eq!(submitted.attempt, 0);
    assert_eq!(submitted.submit_stage, None);
    assert_eq!(submitted.last_error, None);
    assert!(submitted.next_poll_at.is_some());
    let events = ctx.events(&job.id)?;
    assert_eq!(event_types(&events)[2..], ["submitted_via_fallback"]);
    assert_eq!(events[2].payload["token"], "t");
    Ok(())
}

/// The shipped script must report a pre-submit failure (here: playwright cannot be
/// imported) as `submitted: false`, or every such failure would read as an unknown outcome.
#[tokio::test]
async fn shipped_fallback_script_reports_pre_submit_failure_as_definitive() -> Result<()> {
    if !node_available() {
        eprintln!("skipped: node is not available");
        return Ok(());
    }
    let mut ctx = TestContext::start().await?;
    // A copy outside the repo, so `import('playwright')` cannot resolve a local install.
    let script = ctx.tmp.path().join("paperreview_fallback.mjs");
    fs::write(&script, include_str!("../tools/paperreview_fallback.mjs"))?;
    let playwright_resolves = Command::new("node")
        .args(["--input-type=module", "-e", "await import('playwright')"])
        .current_dir(ctx.tmp.path())
        .output()
        .is_ok_and(|out| out.status.success());
    if playwright_resolves {
        eprintln!("skipped: playwright is importable from the temp dir");
        return Ok(());
    }
    ctx.use_fallback_script(&script);
    ctx.mock().push(
        GET_UPLOAD,
        Step::Reply(Reply::json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({ "detail": "forced primary failure" }),
        )),
    );
    let job = ctx.create_queued_job()?;

    assert_eq!(ctx.submit(&job).await?, Attempt::Ran);

    let failed = ctx.job(&job.id)?;
    assert_eq!(failed.status, JobStatus::FailedNeedsManual);
    assert_eq!(failed.submit_stage, None);
    assert!(!failed.fallback_used);
    assert_contains(
        failed.last_error.as_deref(),
        "fallback error: command error",
    );
    assert_contains(failed.last_error.as_deref(), "\"submitted\":false");
    Ok(())
}

// ---------------------------------------------------------------------------------------
// OSS-352: every provider step is identified, inputs are checked before anything is
// sent, and the fallback reports in the primary's terms
// ---------------------------------------------------------------------------------------

/// The `step` recorded on the job's last submit event.
fn last_step(events: &[EventRecord]) -> Option<&str> {
    events
        .last()
        .and_then(|event| event.payload["step"].as_str())
}

/// A Stanford backend on the mock provider whose `step` calls give up after 300 ms.
fn impatient_backend(ctx: &TestContext, step: &str) -> Result<StanfordBackend> {
    let short = StdDuration::from_millis(300);
    let mut timeouts = StepTimeouts::default();
    match step {
        "upload_init" => timeouts.upload_init = short,
        "confirm" => timeouts.confirm = short,
        other => anyhow::bail!("no timeout for step {other}"),
    }
    Ok(StanfordBackend::new(
        ctx.server.base_url.clone(),
        build_client(&ctx.config, None, None, Redirects::Follow)?,
    )
    .with_timeouts(timeouts))
}

#[tokio::test]
async fn upload_init_failure_is_definitive_at_the_upload_init_step() -> Result<()> {
    let ctx = TestContext::start().await?;
    ctx.mock().push(
        GET_UPLOAD,
        Step::Reply(Reply::json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({ "detail": "presign backend down" }),
        )),
    );
    let job = ctx.create_queued_job()?;
    assert_definitive_failure(&ctx, &job, "server error (500): presign backend down").await?;
    assert_eq!(last_step(&ctx.events(&job.id)?), Some("upload_init"));
    assert_eq!(ctx.calls(), [1, 0, 0]);
    Ok(())
}

#[tokio::test]
async fn upload_failure_is_definitive_at_the_upload_step_and_never_confirms() -> Result<()> {
    let ctx = TestContext::start().await?;
    ctx.mock()
        .push(GET_UPLOAD, Step::Reply(ctx.upload_url_reply()));
    ctx.mock().push(
        S3,
        Step::Reply(Reply::text(
            StatusCode::FORBIDDEN,
            "<Error><Code>AccessDenied</Code></Error>",
        )),
    );
    let job = ctx.create_queued_job()?;
    assert_definitive_failure(&ctx, &job, "AccessDenied").await?;
    assert_eq!(last_step(&ctx.events(&job.id)?), Some("upload"));
    assert_eq!(ctx.calls(), [1, 1, 0]);
    Ok(())
}

#[tokio::test]
async fn confirm_rejection_names_the_confirm_step_and_the_validation_detail() -> Result<()> {
    let ctx = TestContext::start().await?;
    ctx.upload_succeeds();
    // FastAPI reports validation failures as a list, as the live service does.
    ctx.confirm(Step::Reply(Reply::json(
        StatusCode::UNPROCESSABLE_ENTITY,
        json!({ "detail": [
            { "type": "missing", "loc": ["body", "email"], "msg": "Field required", "input": null }
        ] }),
    )));
    let job = ctx.create_queued_job()?;
    assert_definitive_failure(&ctx, &job, "email: Field required").await?;
    assert_eq!(last_step(&ctx.events(&job.id)?), Some("confirm"));
    assert_eq!(ctx.calls(), [1, 1, 1]);
    Ok(())
}

#[tokio::test]
async fn unknown_confirm_outcome_names_the_confirm_step() -> Result<()> {
    let ctx = TestContext::start().await?;
    ctx.upload_succeeds();
    ctx.confirm(Step::Reply(Reply::json(
        StatusCode::BAD_GATEWAY,
        json!({ "detail": "upstream" }),
    )));
    let job = ctx.create_queued_job()?;
    assert_eq!(ctx.submit(&job).await?, Attempt::Ran);
    assert_uncertain(
        &ctx.job(&job.id)?,
        "primary",
        &["confirm-upload returned 502"],
    );
    let events = ctx.events(&job.id)?;
    assert_outcome_unknown_event(&events[1], "primary", "confirm-upload returned 502");
    assert_eq!(last_step(&events), Some("confirm"));
    Ok(())
}

#[tokio::test]
async fn upload_init_rate_limit_requeues_at_the_upload_init_step() -> Result<()> {
    let ctx = TestContext::start().await?;
    ctx.mock().push(
        GET_UPLOAD,
        Step::Reply(
            Reply::json(
                StatusCode::TOO_MANY_REQUESTS,
                json!({ "detail": "Rate limit exceeded: 3 per 1 hour" }),
            )
            .header("retry-after", "600"),
        ),
    );
    let job = ctx.create_queued_job()?;
    assert_eq!(ctx.submit(&job).await?, Attempt::Ran);
    let queued = ctx.job(&job.id)?;
    assert_eq!(queued.status, JobStatus::Queued);
    assert_eq!(
        queued.last_error.as_deref(),
        Some("Rate limit exceeded: 3 per 1 hour")
    );
    assert_minutes_from_now(queued.next_poll_at, 9, 10);
    let events = ctx.events(&job.id)?;
    assert_eq!(
        event_types(&events),
        ["submit_dispatched", "submit_rate_limited"]
    );
    assert_eq!(events[1].payload["channel"], "primary");
    assert_eq!(last_step(&events), Some("upload_init"));
    assert_eq!(ctx.calls(), [1, 0, 0]);
    Ok(())
}

#[tokio::test]
async fn hung_upload_init_times_out_as_definitive_without_confirming() -> Result<()> {
    let ctx = TestContext::start().await?;
    ctx.mock().push(GET_UPLOAD, Step::Hang);
    let job = ctx.create_queued_job()?;
    let backend = impatient_backend(&ctx, "upload_init")?;

    let attempt = tokio::time::timeout(
        GUARD,
        worker::submit_job_with_backend(&ctx.config, &ctx.db, &job.id, &backend),
    )
    .await
    .context("submit did not finish")??;
    assert_eq!(attempt, Attempt::Ran);

    let failed = ctx.job(&job.id)?;
    assert_eq!(failed.status, JobStatus::Failed);
    assert_eq!(failed.submit_stage, None);
    assert_contains(failed.last_error.as_deref(), "get-upload-url");
    assert_contains(failed.last_error.as_deref(), "timed out");
    let events = ctx.events(&job.id)?;
    assert_eq!(event_types(&events), ["submit_dispatched", "submit_failed"]);
    assert_eq!(last_step(&events), Some("upload_init"));
    assert_eq!(ctx.calls(), [1, 0, 0]);
    Ok(())
}

#[tokio::test]
async fn hung_confirm_times_out_as_unknown_outcome() -> Result<()> {
    let ctx = TestContext::start().await?;
    ctx.upload_succeeds();
    ctx.confirm(Step::Hang);
    let job = ctx.create_queued_job()?;
    let backend = impatient_backend(&ctx, "confirm")?;

    let attempt = tokio::time::timeout(
        GUARD,
        worker::submit_job_with_backend(&ctx.config, &ctx.db, &job.id, &backend),
    )
    .await
    .context("submit did not finish")??;
    assert_eq!(attempt, Attempt::Ran);

    assert_uncertain(
        &ctx.job(&job.id)?,
        "primary",
        &["confirm-upload got no response"],
    );
    assert_eq!(last_step(&ctx.events(&job.id)?), Some("confirm"));
    assert_eq!(ctx.calls(), [1, 1, 1]);
    Ok(())
}

/// A rejected input is final before dispatch: no provider call, no fallback, and the
/// operator is told what to fix.
async fn assert_input_rejected_before_dispatch(pdf: &[u8], reason: &str) -> Result<()> {
    let mut ctx = TestContext::start().await?;
    let marker = ctx.arm_marker_fallback()?;
    fs::write(&ctx.pdf_path, pdf)?;
    let job = ctx.create_queued_job()?;

    assert_eq!(ctx.submit(&job).await?, Attempt::Ran);

    let failed = ctx.job(&job.id)?;
    assert_eq!(failed.status, JobStatus::FailedNeedsManual);
    assert_eq!(failed.submit_stage, None);
    assert_eq!(failed.attempt, 0);
    assert!(!failed.fallback_used);
    assert_no_lease(&failed);
    assert_contains(failed.last_error.as_deref(), reason);
    let events = ctx.events(&job.id)?;
    assert_eq!(event_types(&events), ["submit_input_rejected"]);
    assert_contains(events[0].payload["reason"].as_str(), reason);
    assert_eq!(ctx.calls(), [0, 0, 0]);
    assert!(!marker.exists(), "fallback ran");
    ctx.assert_ticks_leave_alone(&job, 1).await?;
    Ok(())
}

#[tokio::test]
async fn oversized_pdf_is_rejected_before_any_request() -> Result<()> {
    let mut pdf = b"%PDF-1.4\n".to_vec();
    pdf.resize(10 * 1024 * 1024 + 1, b' ');
    assert_input_rejected_before_dispatch(&pdf, "exceeds the provider's 10 MiB limit").await
}

#[tokio::test]
async fn non_pdf_input_is_rejected_before_any_request() -> Result<()> {
    assert_input_rejected_before_dispatch(b"<html>not a pdf</html>", "not a PDF").await
}

#[tokio::test]
async fn long_paper_records_a_coverage_notice_and_still_submits() -> Result<()> {
    let ctx = TestContext::start().await?;
    let pages = "<< /Type /Page >>\n".repeat(18);
    fs::write(&ctx.pdf_path, format!("%PDF-1.4\n{pages}%%EOF\n"))?;
    ctx.upload_succeeds();
    ctx.confirm(Step::Reply(Reply::json(
        StatusCode::OK,
        json!({ "success": true, "token": "tok-long" }),
    )));
    let job = ctx.create_queued_job()?;

    assert_eq!(ctx.submit(&job).await?, Attempt::Ran);

    assert_eq!(ctx.job(&job.id)?.status, JobStatus::Processing);
    let events = ctx.events(&job.id)?;
    assert_eq!(
        event_types(&events),
        ["submit_input_notice", "submit_dispatched", "submitted"]
    );
    let notice = &events[0].payload;
    assert_eq!(notice["estimated_pages"], 18);
    assert_eq!(notice["reviewed_pages"], 15);
    assert_contains(notice["notices"][0].as_str(), "first 15 pages");
    Ok(())
}

#[tokio::test]
async fn primary_and_fallback_send_the_same_manuscript_email_and_venue() -> Result<()> {
    if !node_available() {
        eprintln!("skipped: node is not available");
        return Ok(());
    }
    let mut ctx = TestContext::start().await?;
    let log = ctx.arm_fallback(FALLBACK_SUCCEEDS)?;
    ctx.upload_succeeds();
    ctx.confirm(Step::Reply(confirm_400()));
    let job = ctx.create_queued_job()?;

    assert_eq!(ctx.submit(&job).await?, Attempt::Ran);
    assert_eq!(ctx.job(&job.id)?.status, JobStatus::Processing);

    let upload_init: Value = serde_json::from_slice(&ctx.mock().bodies(GET_UPLOAD)[0])?;
    let confirm_form = String::from_utf8(ctx.mock().bodies(CONFIRM)[0].clone())?;
    let s3_form = ctx.mock().bodies(S3)[0].clone();
    let runs = fallback_runs(&log)?;
    let argv = &runs[0];

    assert_eq!(upload_init["filename"], "paper.pdf");
    assert_eq!(arg_after(argv, "--filename"), Some("paper.pdf"));
    assert_eq!(upload_init["venue"], "ICLR");
    assert_eq!(arg_after(argv, "--venue"), Some("ICLR"));
    assert!(confirm_form.contains(EMAIL), "confirm form: {confirm_form}");
    assert!(
        confirm_form.contains("ICLR"),
        "confirm form: {confirm_form}"
    );
    assert_eq!(arg_after(argv, "--email"), Some(EMAIL));
    // Both channels upload the job's pinned snapshot.
    let snapshot = fs::read(arg_after(argv, "--pdf").context("--pdf")?)?;
    assert!(
        s3_form
            .windows(snapshot.len())
            .any(|window| window == snapshot),
        "the S3 upload must carry the snapshot bytes"
    );
    Ok(())
}

#[tokio::test]
async fn fallback_rate_limit_requeues_like_the_primary() -> Result<()> {
    let Some((ctx, job, _log)) = run_fallback(
        r#"console.error(JSON.stringify({ success: false, submitted: false, stage: "upload_init", rate_limited: true, retry_after_secs: 120, error: "Rate limit exceeded" })); process.exit(1);"#,
    )
    .await?
    else {
        return Ok(());
    };
    let queued = ctx.job(&job.id)?;
    assert_eq!(queued.status, JobStatus::Queued);
    assert_eq!(queued.attempt, 1);
    assert_eq!(queued.submit_stage, None);
    assert!(
        !queued.fallback_used,
        "a rate-limited fallback created nothing, so a retry may use it again"
    );
    assert_contains(queued.last_error.as_deref(), "Rate limit exceeded");
    assert_minutes_from_now(queued.next_poll_at, 1, 2);
    let events = ctx.events(&job.id)?;
    assert_eq!(event_types(&events)[2..], ["submit_rate_limited"]);
    assert_eq!(events[2].payload["channel"], "fallback");
    assert_eq!(events[2].payload["retry_after_source"], "server");
    assert_eq!(last_step(&events), Some("upload_init"));
    Ok(())
}

#[tokio::test]
async fn fallback_confirm_rejection_is_definitive_not_uncertain() -> Result<()> {
    assert_fallback_fails_needs_manual(
        r#"console.error(JSON.stringify({ success: false, submitted: true, stage: "confirm", status: 400, error: "Invalid email address" })); process.exit(1);"#,
        "Invalid email address",
    )
    .await
}

#[tokio::test]
async fn fallback_confirm_server_error_stays_uncertain() -> Result<()> {
    assert_fallback_parks_uncertain(
        r#"console.error(JSON.stringify({ success: false, submitted: true, stage: "confirm", status: 502, error: "Bad gateway" })); process.exit(1);"#,
        "Bad gateway",
    )
    .await
}

/// The shipped script's classifier, given the network facts of each provider outcome,
/// reports what the Rust side maps to the primary's semantics: 429 → rate limited,
/// nothing confirmed or a 4xx → definitive, a confirm without a definite answer → unknown.
#[tokio::test]
async fn shipped_fallback_script_classifies_outcomes_like_the_primary() -> Result<()> {
    if !node_available() {
        eprintln!("skipped: node is not available");
        return Ok(());
    }
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("tools/paperreview_fallback.mjs");
    let url = format!("file://{}", script.canonicalize()?.display());
    let program = format!(
        r#"import {{ classify, newFacts }} from {url};
const at = (stage, status, extra = {{}}) => ({{
  ...newFacts(), stage, confirmSent: stage === 'confirm',
  responses: {{ [stage]: {{ status, detail: 'detail ' + status, ...extra }} }},
}});
console.log(JSON.stringify([
  classify({{ ...newFacts(), stage: 'confirm', confirmSent: true, token: 'tok' }}),
  classify(at('upload_init', 429, {{ retryAfterSecs: 60 }})),
  classify(at('upload', 403)),
  classify(at('confirm', 422)),
  classify(at('confirm', 502)),
  classify({{ ...newFacts(), stage: 'confirm', confirmSent: true, confirmFailed: 'confirm-upload got no response: net::ERR_CONNECTION_RESET' }}),
  classify({{ ...newFacts(), dialog: 'File size exceeds 10MB limit.' }}),
  classify(newFacts(), new Error('playwright missing')),
]));"#,
        url = json!(url)
    );
    let out = Command::new("node")
        .args(["--input-type=module", "-e", &program])
        .output()?;
    assert!(
        out.status.success(),
        "node failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let reports: Vec<Value> = serde_json::from_slice(&out.stdout)?;
    let [
        accepted,
        limited,
        upload_rejected,
        confirm_rejected,
        confirm_5xx,
        confirm_lost,
        alerted,
        no_browser,
    ] = reports.as_slice()
    else {
        anyhow::bail!("unexpected reports: {reports:?}");
    };

    assert_eq!(accepted["success"], true);
    assert_eq!(accepted["token"], "tok");

    assert_eq!(limited["rate_limited"], true);
    assert_eq!(limited["retry_after_secs"], 60);
    assert_eq!(limited["submitted"], false);
    assert_eq!(limited["stage"], "upload_init");

    assert_eq!(upload_rejected["submitted"], false);
    assert_eq!(upload_rejected["stage"], "upload");
    assert_eq!(upload_rejected["status"], 403);

    assert_eq!(confirm_rejected["submitted"], true);
    assert_eq!(confirm_rejected["status"], 422);
    assert_eq!(confirm_rejected["error"], "detail 422");

    assert_eq!(confirm_5xx["submitted"], true);
    assert_eq!(confirm_5xx["status"], 502);

    assert_eq!(confirm_lost["submitted"], true);
    assert!(confirm_lost.get("status").is_none(), "{confirm_lost}");
    assert_contains(confirm_lost["error"].as_str(), "got no response");

    assert_eq!(alerted["submitted"], false);
    assert_eq!(alerted["error"], "File size exceeds 10MB limit.");

    assert_eq!(no_browser["submitted"], false);
    assert_eq!(no_browser["stage"], Value::Null);
    assert_contains(no_browser["error"].as_str(), "playwright missing");
    Ok(())
}
