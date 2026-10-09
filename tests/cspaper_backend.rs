//! OSS-353 acceptance, end to end through the real CSPaper backend against a mock
//! CSPaper API: receipts and every job status, definitive refusals (credentials,
//! template, payload), rate limits, unknown outcomes parked as SUBMITTED/UNCERTAIN and
//! never resent, refused redirects, restart and crash recovery, the shared review
//! operations, and an API key that never leaves the `X-API-Key` header.

mod common;

use anyhow::{Context, Result};
use axum::{
    Router,
    body::to_bytes,
    extract::{Request, State},
    http::{HeaderMap, Method, StatusCode, header},
    response::{IntoResponse, Response},
    routing::any,
};
use chrono::{DateTime, Duration, Utc};
use common::{
    Ctx, PAPER, PROJECT, assert_no_lease, assert_secret_absent, diagnostic, dispatch_channels,
    event_types, only_event,
};
use reviewloop::{
    application::{
        Approval, OpError, RequestDisposition, RequestOrigin, ReviewOps, ReviewPart, ReviewQuery,
        ReviewRequest, ReviewRequestOutcome,
    },
    config::{Config, PaperConfig, Redacted},
    db::{ClaimTiming, Db, Lease, LeaseRecovery},
    model::{ExistingReason, Job, JobPdf, JobStatus, NewJob, ReviewOptions, SubmitStage, WorkKind},
    submission_input::prepare_input,
    worker::{self, Attempt},
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    fmt::Debug,
    fs,
    future::Future,
    net::SocketAddr,
    ops::{Deref, DerefMut},
    panic::AssertUnwindSafe,
    path::Path,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration as StdDuration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Notify,
    task::{JoinHandle, JoinSet},
};

/// The organisation key of every test. It may only ever travel in the request header.
const SECRET: &str = "csp_live_TESTSECRET_do_not_leak";
/// A key the mock accepts that is not the configured one: the configured key is invalid.
const OTHER_ORG_KEY: &str = "csp_live_ANOTHER_ORG_KEY";
/// Review template (`agent_id`) the fixtures were recorded for.
const AGENT: &str = "ICLR_main_2026_1";
const OTHER_AGENT: &str = "NeurIPS_main_2026_1";
/// The CSPaper job id in `submit_accepted.json` and every `review_*.json` fixture.
const JOB_ID: &str = "856f388c-d5cd-4409-b3bf-3e0c8279dc49";
/// `paper_meta.title` of the completed fixture.
const TITLE: &str = "A Study of Agentic Peer Review";
const DESK_REJECTION: &str = "desk_rejection_enabled";
/// Every reconcile hint for a CSPaper submission without a receipt names these.
const CSPAPER_RECONCILE_HINTS: [&str; 4] = [
    "CSPaper sends no email",
    "import-token",
    "--force",
    "cancel",
];

const SUBMIT_PATH: &str = "/api/platform/review";
const REVIEWS_PATH: &str = "/api/platform/reviews";
const API_KEY_HEADER: &str = "x-api-key";
const SUBMIT: &str = "submit";
const POLL: &str = "poll";
/// Any request outside the two API routes.
const OTHER: &str = "other";
/// Upper bound on any wait on the worker or the mock; the worker's own dispatch timeout
/// is 20 minutes, so hitting this means a test is exercising the wrong path.
const GUARD: StdDuration = StdDuration::from_secs(60);

// ---------------------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------------------

fn fixture(name: &str) -> Value {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/cspaper")
        .join(name);
    let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// A job detail fixture describing CSPaper job `job_id`.
fn job_body(name: &str, job_id: &str) -> Value {
    let mut body = fixture(name);
    body["data"]["id"] = json!(job_id);
    body
}

// ---------------------------------------------------------------------------------------
// Mock CSPaper API
// ---------------------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct Reply {
    status: StatusCode,
    body: String,
    content_type: &'static str,
    headers: Vec<(&'static str, String)>,
}

impl Reply {
    fn json(status: StatusCode, value: &Value) -> Self {
        Self {
            status,
            body: value.to_string(),
            content_type: "application/json",
            headers: vec![],
        }
    }

    fn html(status: StatusCode, body: &str) -> Self {
        Self {
            status,
            body: body.to_string(),
            content_type: "text/html",
            headers: vec![],
        }
    }

    fn fixture(status: StatusCode, name: &str) -> Self {
        Self::json(status, &fixture(name))
    }

    /// CSPaper's error envelope.
    fn error(status: StatusCode, message: &str) -> Self {
        Self::json(
            status,
            &json!({ "status": status.as_u16(), "data": { "message": message, "details": null } }),
        )
    }

    /// The documented `202` receipt, naming CSPaper job `job_id`.
    fn accepted(job_id: &str) -> Self {
        let mut body = fixture("submit_accepted.json");
        body["data"]["job_id"] = json!(job_id);
        Self::json(StatusCode::ACCEPTED, &body)
    }

    /// `200` with the job detail fixture `name` for CSPaper job `job_id`.
    fn job(name: &str, job_id: &str) -> Self {
        Self::json(StatusCode::OK, &job_body(name, job_id))
    }

    /// `name` must be lowercase.
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

/// What a route does with the next authenticated request it receives.
enum Step {
    Reply(Reply),
    /// Signal `entered`, wait for `release`, then reply: the request has reached the
    /// provider and the test acts while the worker awaits the receipt.
    Gate(Reply),
    /// Signal `entered` and never answer.
    Hang,
}

/// One part of a `multipart/form-data` body.
#[derive(Clone, Debug, Default)]
struct FormPart {
    name: String,
    file_name: Option<String>,
    content_type: Option<String>,
    bytes: Vec<u8>,
}

impl FormPart {
    fn new(head: &str, bytes: Vec<u8>) -> Self {
        let mut part = Self {
            bytes,
            ..Self::default()
        };
        for line in head.split("\r\n") {
            let Some((name, value)) = line.split_once(':') else {
                continue;
            };
            match name.trim().to_ascii_lowercase().as_str() {
                "content-disposition" => {
                    for param in value.split(';').skip(1) {
                        let Some((key, raw)) = param.split_once('=') else {
                            continue;
                        };
                        let raw = raw.trim().trim_matches('"').to_string();
                        match key.trim() {
                            "name" => part.name = raw,
                            "filename" => part.file_name = Some(raw),
                            _ => {}
                        }
                    }
                }
                "content-type" => part.content_type = Some(value.trim().to_string()),
                _ => {}
            }
        }
        part
    }
}

fn find_bytes(haystack: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    haystack
        .get(from..)?
        .windows(needle.len())
        .position(|window| window == needle)
        .map(|offset| offset + from)
}

/// Split a `multipart/form-data` body into its parts; empty for any other body.
fn parse_multipart(content_type: &str, body: &[u8]) -> Vec<FormPart> {
    let Some(boundary) = content_type.split("boundary=").nth(1) else {
        return Vec::new();
    };
    let boundary = boundary.split(';').next().unwrap_or_default();
    let delimiter = format!("\r\n--{}", boundary.trim().trim_matches('"'));
    // A leading CRLF lets the first delimiter match like every later one.
    let body = [b"\r\n".as_slice(), body].concat();
    let mut parts = Vec::new();
    let Some(first) = find_bytes(&body, delimiter.as_bytes(), 0) else {
        return parts;
    };
    let mut at = first + delimiter.len();
    // Each delimiter is followed by CRLF and a part, or by `--` after the last part.
    while body[at..].starts_with(b"\r\n") {
        let head_start = at + 2;
        let Some(head_end) = find_bytes(&body, b"\r\n\r\n", head_start) else {
            break;
        };
        let Some(next) = find_bytes(&body, delimiter.as_bytes(), head_end + 4) else {
            break;
        };
        let head = String::from_utf8_lossy(&body[head_start..head_end]);
        parts.push(FormPart::new(&head, body[head_end + 4..next].to_vec()));
        at = next + delimiter.len();
    }
    parts
}

/// A request as the mock received it.
#[derive(Clone, Debug)]
struct Received {
    route: &'static str,
    method: Method,
    uri: String,
    headers: HeaderMap,
    body: Vec<u8>,
    form: Vec<FormPart>,
}

impl Received {
    fn api_key(&self) -> Option<&str> {
        self.headers
            .get(API_KEY_HEADER)
            .and_then(|value| value.to_str().ok())
    }

    fn part(&self, name: &str) -> Option<&FormPart> {
        self.form.iter().find(|part| part.name == name)
    }

    fn text(&self, name: &str) -> Option<String> {
        self.part(name)
            .map(|part| String::from_utf8_lossy(&part.bytes).into_owned())
    }

    fn part_names(&self) -> BTreeSet<&str> {
        self.form.iter().map(|part| part.name.as_str()).collect()
    }
}

struct MockState {
    /// The organisation key the mock accepts; any other gets the live 403.
    accepted_key: Mutex<String>,
    /// Quote the refused key back in the 403 message, as a careless server might.
    echo_refused_key: AtomicBool,
    steps: Mutex<HashMap<&'static str, VecDeque<Step>>>,
    received: Mutex<Vec<Received>>,
    entered: Notify,
    release: Notify,
}

impl Default for MockState {
    fn default() -> Self {
        Self {
            accepted_key: Mutex::new(SECRET.to_string()),
            echo_refused_key: AtomicBool::new(false),
            steps: Mutex::default(),
            received: Mutex::default(),
            entered: Notify::new(),
            release: Notify::new(),
        }
    }
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

    /// Refuse the configured key from now on.
    fn accept_only(&self, key: &str) {
        *self.accepted_key.lock().unwrap() = key.to_string();
    }

    fn echo_refused_key(&self) {
        self.echo_refused_key.store(true, Ordering::SeqCst);
    }

    fn received(&self, route: &'static str) -> Vec<Received> {
        self.received
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request.route == route)
            .cloned()
            .collect()
    }

    fn only(&self, route: &'static str) -> Received {
        let mut requests = self.received(route);
        assert_eq!(requests.len(), 1, "expected exactly one {route} request");
        requests.remove(0)
    }

    /// `[submit, poll, other]` request counts.
    fn calls(&self) -> [usize; 3] {
        [SUBMIT, POLL, OTHER].map(|route| self.received(route).len())
    }

    /// Every request carried `secret` in its key header and nowhere else.
    fn assert_key_confined(&self, secret: &str) {
        for request in self.received.lock().unwrap().iter() {
            assert!(!request.uri.contains(secret), "key in URL {}", request.uri);
            for (name, value) in &request.headers {
                if name.as_str() != API_KEY_HEADER {
                    assert!(
                        !String::from_utf8_lossy(value.as_bytes()).contains(secret),
                        "key in header {name}"
                    );
                }
            }
            assert!(
                find_bytes(&request.body, secret.as_bytes(), 0).is_none(),
                "key in the {} body",
                request.route
            );
        }
    }

    async fn serve(&self, route: &'static str, req: Request) -> Response {
        let (head, body) = req.into_parts();
        // Read the whole body first, so a gated request has provably been delivered.
        let body = to_bytes(body, usize::MAX)
            .await
            .map(|bytes| bytes.to_vec())
            .unwrap_or_default();
        let content_type = head
            .headers
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        let received = Received {
            route,
            method: head.method.clone(),
            uri: head.uri.to_string(),
            form: parse_multipart(content_type, &body),
            headers: head.headers.clone(),
            body,
        };
        let key = received.api_key().map(str::to_string);
        self.received.lock().unwrap().push(received);
        if route == OTHER {
            return Reply::error(StatusCode::NOT_FOUND, "no such route").into_response();
        }

        // The live service checks the key before anything else.
        let accepted = self.accepted_key.lock().unwrap().clone();
        match key {
            None => {
                return Reply::fixture(StatusCode::UNAUTHORIZED, "error_401.json").into_response();
            }
            Some(key) if key != accepted => {
                if self.echo_refused_key.load(Ordering::SeqCst) {
                    let message = format!("Invalid API Key: {key}");
                    return Reply::error(StatusCode::FORBIDDEN, &message).into_response();
                }
                return Reply::fixture(StatusCode::FORBIDDEN, "error_403.json").into_response();
            }
            Some(_) => {}
        }

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
            None => Reply::error(
                StatusCode::IM_A_TEAPOT,
                &format!("mock {route} has no scripted reply"),
            )
            .into_response(),
        }
    }
}

/// axum app behind a TCP relay that owns every client connection. `axum::serve` runs
/// each connection in its own task, so aborting it would not drop an in-flight request;
/// aborting the relay drops both ends of every connection and stops accepting new ones, as
/// if the provider vanished mid-request.
struct MockServer {
    base_url: String,
    state: Arc<MockState>,
    app: JoinHandle<()>,
    relay: Mutex<Option<JoinHandle<()>>>,
}

impl MockServer {
    async fn start() -> Result<Self> {
        let state = Arc::new(MockState::default());
        // `any` method, so a request that slips past the expected verb is still counted.
        let app = Router::new()
            .route(
                SUBMIT_PATH,
                any(
                    |State(state): State<Arc<MockState>>, req: Request| async move {
                        state.serve(SUBMIT, req).await
                    },
                ),
            )
            .route(
                &format!("{REVIEWS_PATH}/{{job_id}}"),
                any(
                    |State(state): State<Arc<MockState>>, req: Request| async move {
                        state.serve(POLL, req).await
                    },
                ),
            )
            .fallback(
                |State(state): State<Arc<MockState>>, req: Request| async move {
                    state.serve(OTHER, req).await
                },
            )
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
            state,
            app,
            relay: Mutex::new(Some(relay)),
        })
    }

    /// Drop every open connection without a response and stop accepting new ones.
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

/// The project config of every test: one cspaper paper reviewed on the mock with the
/// key and default template set; no triggers, notifications or email; schedule
/// `[10, 20, 40, 60]` minutes without jitter. A free function, so a restart can build
/// a fresh one.
fn cspaper_config(state_dir: &Path, pdf_path: &Path, base_url: &str) -> Config {
    let mut config = Config {
        project_id: PROJECT.to_string(),
        ..Config::default()
    };
    config.core.state_dir = state_dir.to_string_lossy().to_string();
    config.core.widget_state_enabled = false;
    config.polling.schedule_minutes = vec![10, 20, 40, 60];
    config.polling.jitter_percent = 0;
    config.trigger.git.enabled = false;
    config.trigger.pdf.enabled = false;
    config.imap = None;
    config.notifications.enabled = false;
    // A stray Stanford call fails fast.
    config.providers.stanford.base_url = "http://127.0.0.1:9".to_string();
    config.providers.stanford.fallback_mode = "disabled".to_string();
    config.providers.cspaper.base_url = base_url.to_string();
    config.providers.cspaper.api_key = Some(Redacted(SECRET.to_string()));
    config.providers.cspaper.agent_id = Some(AGENT.to_string());
    config.providers.cspaper.desk_rejection_enabled = true;
    config.papers = vec![PaperConfig {
        id: PAPER.to_string(),
        pdf_path: pdf_path.to_string_lossy().to_string(),
        backend: "cspaper".to_string(),
        venue: None,
    }];
    config
}

/// The shared test project, reconfigured for cspaper on a [`MockServer`].
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
    async fn start() -> Result<Self> {
        let server = MockServer::start().await?;
        let mut ctx = Ctx::new()?;
        ctx.config = cspaper_config(&ctx.config.state_dir(), &ctx.pdf_path, &server.base_url);
        Ok(Self { ctx, server })
    }

    fn mock(&self) -> &MockState {
        &self.server.state
    }

    fn push(&self, route: &'static str, step: Step) {
        self.mock().push(route, step);
    }

    fn reply(&self, route: &'static str, reply: Reply) {
        self.push(route, Step::Reply(reply));
    }

    /// `[submit, poll, other]` request counts.
    fn calls(&self) -> [usize; 3] {
        self.mock().calls()
    }

    fn ops(&self) -> ReviewOps<'_> {
        ReviewOps::new(&self.config, &self.db)
    }

    /// Ask for a review of the paper through the shared operations, as an agent does.
    fn request(&self, request_key: Option<&str>) -> Result<ReviewRequestOutcome, OpError> {
        self.ops().request_review(&ReviewRequest {
            paper_id: PAPER.to_string(),
            request_key: request_key.map(str::to_string),
            force: false,
            approval: Approval::Granted,
            origin: RequestOrigin::Agent,
        })
    }

    /// A new QUEUED job, enqueued through the shared operations.
    fn enqueue(&self) -> Result<Job> {
        let outcome = self.request(None)?;
        assert_eq!(outcome.disposition, RequestDisposition::Created);
        self.job(&outcome.job.job_id)
    }

    /// A QUEUED cspaper job inserted without the enqueue checks, like one left from
    /// before a setting was removed.
    fn insert_job(&self, venue: Option<&str>) -> Result<Job> {
        let paper = self.config.find_paper(PAPER).context("paper configured")?;
        self.db.create_job(&NewJob {
            project_id: PROJECT.to_string(),
            paper_id: PAPER.to_string(),
            backend: "cspaper".to_string(),
            pdf: JobPdf::Pinned(prepare_input(&self.config.state_dir(), &self.pdf_path)?),
            status: JobStatus::Queued,
            email: String::new(),
            venue: venue.map(str::to_string),
            review_options: self.config.review_options_for(paper),
            git_tag: None,
            git_commit: None,
            next_poll_at: None,
        })
    }

    /// A PROCESSING job holding CSPaper job [`JOB_ID`], submitted through the mock.
    async fn submitted(&self) -> Result<Job> {
        let job = self.enqueue()?;
        self.reply(SUBMIT, Reply::accepted(JOB_ID));
        assert_eq!(self.submit(&job.id).await?, Attempt::Ran);
        let submitted = self.job(&job.id)?;
        assert_eq!(submitted.status, JobStatus::Processing);
        assert_eq!(submitted.token.as_deref(), Some(JOB_ID));
        Ok(submitted)
    }

    /// Submit through the real CSPaper backend, as `reviewloop submit` does.
    async fn submit(&self, job_id: &str) -> Result<Attempt> {
        tokio::time::timeout(GUARD, worker::submit_job(&self.config, &self.db, job_id))
            .await
            .context("submit did not finish")?
    }

    /// Poll through the real CSPaper backend, as `reviewloop check` does.
    async fn poll(&self, job_id: &str) -> Result<Attempt> {
        tokio::time::timeout(GUARD, worker::poll_job(&self.config, &self.db, job_id))
            .await
            .context("poll did not finish")?
    }

    async fn tick(&self) -> Result<()> {
        tokio::time::timeout(GUARD, worker::run_tick(&self.config, &self.db))
            .await
            .context("tick did not finish")?
    }

    /// Make the job's next submit or poll due now, keeping its status.
    fn make_due(&self, job_id: &str, status: JobStatus) -> Result<()> {
        self.db.update_job_state(
            job_id,
            status,
            None,
            Some(Some(Utc::now() - Duration::seconds(1))),
            None,
        )
    }

    /// Run daemon ticks and check they leave the job, its events and the provider alone.
    async fn assert_ticks_leave_alone(&self, job_id: &str, ticks: usize) -> Result<()> {
        let before = serde_json::to_value(self.job(job_id)?)?;
        let events_before = self.events(job_id)?.len();
        let calls_before = self.calls();
        for _ in 0..ticks {
            self.tick().await?;
        }
        assert_eq!(serde_json::to_value(self.job(job_id)?)?, before);
        assert_eq!(self.events(job_id)?.len(), events_before);
        assert_eq!(self.calls(), calls_before, "a tick contacted the provider");
        Ok(())
    }

    /// Nothing the project stores or prints holds the key, and it reached the provider
    /// only in its header.
    fn assert_secret_contained(&self) -> Result<()> {
        assert_secret_absent(&self.ctx, SECRET)?;
        self.mock().assert_key_confined(SECRET);
        Ok(())
    }
}

// ---------------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------------

/// Drive `worker` until a gated request reaches the mock. The worker stays alive.
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
    .context("the gated request never reached the mock")?
}

fn assert_contains(haystack: Option<&str>, needle: &str) {
    let haystack = haystack.unwrap_or_default();
    assert!(
        haystack.contains(needle),
        "expected {needle:?} in {haystack:?}"
    );
}

fn assert_minutes_from_now(at: Option<DateTime<Utc>>, min: i64, max: i64) {
    let minutes = (at.expect("next_poll_at set") - Utc::now()).num_minutes();
    assert!(
        (min..=max).contains(&minutes),
        "expected next_poll_at in [{min}, {max}] minutes, got {minutes}"
    );
}

fn assert_seconds_after(at: Option<DateTime<Utc>>, from: DateTime<Utc>, min: i64, max: i64) {
    let seconds = (at.expect("next_poll_at set") - from).num_seconds();
    assert!(
        (min..=max).contains(&seconds),
        "expected a delay in [{min}, {max}] s, got {seconds} s"
    );
}

fn read_json(path: &Path) -> Result<Value> {
    let text = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    Ok(serde_json::from_str(&text)?)
}

fn desk_rejection(value: &str) -> ReviewOptions {
    ReviewOptions::default().with(DESK_REJECTION, value)
}

// ---------------------------------------------------------------------------------------
// Submission receipts
// ---------------------------------------------------------------------------------------

#[tokio::test]
async fn accepted_receipt_moves_to_processing_with_the_job_id_as_token() -> Result<()> {
    let ctx = TestContext::start().await?;
    let job = ctx.enqueue()?;
    assert_eq!(job.venue.as_deref(), Some(AGENT));
    assert_eq!(job.review_options, desk_rejection("true"));
    let snapshot = job.snapshot_path.clone().context("pinned snapshot")?;
    let snapshot_bytes = fs::read(&snapshot)?;
    // The paper changes after enqueue; the job still uploads what it was asked for.
    fs::write(&ctx.pdf_path, b"%PDF-1.7\n% edited after enqueue\n%%EOF\n")?;
    ctx.reply(SUBMIT, Reply::accepted(JOB_ID));

    assert_eq!(ctx.submit(&job.id).await?, Attempt::Ran);

    let submitted = ctx.job(&job.id)?;
    assert_eq!(submitted.status, JobStatus::Processing);
    assert_eq!(submitted.token.as_deref(), Some(JOB_ID));
    assert_eq!(submitted.attempt, 0);
    assert_eq!(submitted.submit_stage, None);
    assert_eq!(submitted.last_error, None);
    assert!(!submitted.fallback_used);
    assert!(submitted.started_at.is_some());
    assert_no_lease(&submitted);
    assert_minutes_from_now(submitted.next_poll_at, 9, 10);
    let events = ctx.events(&job.id)?;
    assert_eq!(
        event_types(&events),
        ["job_enqueued", "submit_dispatched", "submitted"]
    );
    assert_eq!(dispatch_channels(&events), ["primary"]);
    let receipt = only_event(&events, "submitted");
    assert_eq!(receipt["backend"], "cspaper");
    assert_eq!(receipt["channel"], "primary");
    assert_eq!(receipt["token"], JOB_ID);

    let post = ctx.mock().only(SUBMIT);
    assert_eq!(post.method, Method::POST);
    assert_eq!(post.uri, SUBMIT_PATH);
    assert_eq!(post.api_key(), Some(SECRET));
    // The template, the desk-rejection switch and the PDF; no submitter email.
    assert_eq!(
        post.part_names(),
        BTreeSet::from(["agent_id", DESK_REJECTION, "file"])
    );
    assert_eq!(post.text("agent_id").as_deref(), Some(AGENT));
    assert_eq!(post.text(DESK_REJECTION).as_deref(), Some("true"));
    let file = post.part("file").context("file part")?;
    assert_eq!(file.file_name.as_deref(), Some("paper.pdf"));
    assert_eq!(Path::new(&snapshot).file_name(), Some("paper.pdf".as_ref()));
    assert_eq!(file.content_type.as_deref(), Some("application/pdf"));
    assert_eq!(file.bytes, snapshot_bytes);
    assert_ne!(file.bytes, fs::read(&ctx.pdf_path)?);
    assert_eq!(ctx.calls(), [1, 0, 0]);
    Ok(())
}

#[tokio::test]
async fn desk_rejection_setting_recorded_at_enqueue_is_sent() -> Result<()> {
    let mut ctx = TestContext::start().await?;
    ctx.config.providers.cspaper.desk_rejection_enabled = false;
    let job = ctx.enqueue()?;
    assert_eq!(job.review_options, desk_rejection("false"));
    // The job keeps what was asked for; the setting at submit time does not matter.
    ctx.config.providers.cspaper.desk_rejection_enabled = true;
    ctx.reply(SUBMIT, Reply::accepted(JOB_ID));

    assert_eq!(ctx.submit(&job.id).await?, Attempt::Ran);

    assert_eq!(ctx.job(&job.id)?.status, JobStatus::Processing);
    let post = ctx.mock().only(SUBMIT);
    assert_eq!(post.text(DESK_REJECTION).as_deref(), Some("false"));
    assert_eq!(post.text("agent_id").as_deref(), Some(AGENT));
    Ok(())
}

#[tokio::test]
async fn paper_venue_overrides_the_default_template() -> Result<()> {
    let mut ctx = TestContext::start().await?;
    ctx.config.papers[0].venue = Some(OTHER_AGENT.to_string());
    let job = ctx.enqueue()?;
    assert_eq!(job.venue.as_deref(), Some(OTHER_AGENT));
    ctx.reply(SUBMIT, Reply::accepted(JOB_ID));

    assert_eq!(ctx.submit(&job.id).await?, Attempt::Ran);

    assert_eq!(ctx.job(&job.id)?.status, JobStatus::Processing);
    let post = ctx.mock().only(SUBMIT);
    assert_eq!(post.text("agent_id").as_deref(), Some(OTHER_AGENT));
    Ok(())
}

// ---------------------------------------------------------------------------------------
// Polling
// ---------------------------------------------------------------------------------------

#[tokio::test]
async fn polls_keep_processing_until_completed_and_archive_the_review() -> Result<()> {
    let ctx = TestContext::start().await?;
    let job = ctx.submitted().await?;
    ctx.reply(POLL, Reply::job("review_pending.json", JOB_ID));
    ctx.reply(POLL, Reply::job("review_processing.json", JOB_ID));

    for attempt in 1..=2 {
        assert_eq!(ctx.poll(&job.id).await?, Attempt::Ran);
        let polled = ctx.job(&job.id)?;
        assert_eq!(polled.status, JobStatus::Processing);
        assert_eq!(polled.attempt, attempt);
        assert_eq!(polled.token.as_deref(), Some(JOB_ID));
        assert_eq!(polled.last_error, None);
        assert_no_lease(&polled);
    }
    let events = ctx.events(&job.id)?;
    let polled: Vec<&Value> = events
        .iter()
        .filter(|event| event.event_type == "poll_processing")
        .map(|event| &event.payload["attempt"])
        .collect();
    assert_eq!(polled, [&json!(1), &json!(2)]);

    // The review completes at the daemon's next due poll.
    let completed_body = job_body("review_completed.json", JOB_ID);
    ctx.reply(POLL, Reply::json(StatusCode::OK, &completed_body));
    ctx.make_due(&job.id, JobStatus::Processing)?;
    ctx.tick().await?;

    let done = ctx.job(&job.id)?;
    assert_eq!(done.status, JobStatus::Completed);
    assert_eq!(done.attempt, 3);
    assert_eq!(done.token.as_deref(), Some(JOB_ID));
    assert_eq!(done.next_poll_at, None);
    assert_eq!(done.last_error, None);
    assert_no_lease(&done);
    let events = ctx.events(&job.id)?;
    assert_eq!(event_types(&events).last(), Some(&"review_completed"));
    assert_eq!(only_event(&events, "review_completed")["token"], JOB_ID);

    let polls = ctx.mock().received(POLL);
    assert_eq!(polls.len(), 3);
    for poll in &polls {
        assert_eq!(poll.method, Method::GET);
        assert_eq!(poll.uri, format!("{REVIEWS_PATH}/{JOB_ID}"));
        assert_eq!(poll.api_key(), Some(SECRET));
    }
    assert_eq!(ctx.calls(), [1, 3, 0]);

    // The archive keeps the provider's answer verbatim beside the normalized fields.
    let dir = ctx.config.state_dir().join("artifacts").join(&job.id);
    let review = read_json(&dir.join("review.json"))?;
    assert_eq!(review["provider_raw"], completed_body);
    assert_eq!(review["provider"], "cspaper");
    assert_eq!(review["provider_job_id"], JOB_ID);
    assert_eq!(review["agent_id"], AGENT);
    assert_eq!(review["venue"], AGENT);
    assert_eq!(review["title"], TITLE);
    assert_eq!(review["numerical_score"], json!(7.5));
    assert_eq!(review["desk_reject"], json!(false));
    assert_eq!(review["result_summary"]["overall_score"], json!(7.5));
    let result = completed_body["data"]["result"]
        .as_str()
        .context("fixture result")?;
    assert_eq!(review["content"], result);
    let markdown = fs::read_to_string(dir.join("review.md"))?;
    assert!(markdown.contains(result), "{markdown}");
    assert!(markdown.contains(TITLE), "{markdown}");
    assert!(!markdown.contains("Raw JSON"), "{markdown}");
    let meta = read_json(&dir.join("meta.json"))?;
    assert_eq!(meta["job_id"], json!(job.id));
    assert_eq!(meta["paper_id"], PAPER);
    assert_eq!(meta["backend"], "cspaper");
    assert_eq!(meta["token"], JOB_ID);
    assert_eq!(meta["venue"], AGENT);
    assert_eq!(meta["review_options"], json!({ DESK_REJECTION: "true" }));
    assert_eq!(meta["version_no"], json!(1));
    assert_eq!(meta["round_no"], json!(1));
    assert_eq!(meta["version_source"], "pdf_hash");
    assert_eq!(meta["version_key"], json!(done.pdf_hash));
    assert_eq!(meta["pdf_hash"], json!(done.pdf_hash));
    assert_eq!(meta["snapshot_path"], json!(done.snapshot_path));
    let stored = ctx.db.get_review(&job.id)?.context("stored review")?;
    assert_eq!(stored.raw_json, review);

    let view = ctx.ops().get_review(&ReviewQuery {
        job_id: job.id.clone(),
        part: ReviewPart::Markdown,
    })?;
    assert_eq!(view.job.status, JobStatus::Completed);
    assert_eq!(view.score.as_deref(), Some("7.5"));
    assert_eq!(view.title.as_deref(), Some(TITLE));
    assert_contains(view.markdown.as_deref(), "## Strengths");
    ctx.assert_secret_contained()
}

#[tokio::test]
async fn provider_failure_needs_manual_and_is_never_polled_again() -> Result<()> {
    let ctx = TestContext::start().await?;
    let job = ctx.submitted().await?;
    ctx.reply(POLL, Reply::job("review_failed.json", JOB_ID));

    assert_eq!(ctx.poll(&job.id).await?, Attempt::Ran);

    let failed = ctx.job(&job.id)?;
    assert_eq!(failed.status, JobStatus::FailedNeedsManual);
    assert_eq!(failed.attempt, 1);
    assert_eq!(failed.token.as_deref(), Some(JOB_ID));
    assert_eq!(failed.next_poll_at, None);
    assert_no_lease(&failed);
    let last_error = diagnostic(&failed)?;
    assert_contains(
        Some(last_error),
        "provider reported the review failed: LLM resource exhausted",
    );
    assert_contains(
        Some(last_error),
        &format!("reviewloop submit --paper-id {PAPER}"),
    );
    let events = ctx.events(&job.id)?;
    assert_eq!(event_types(&events).last(), Some(&"poll_provider_failed"));
    assert_eq!(
        only_event(&events, "poll_provider_failed")["reason"],
        "LLM resource exhausted"
    );
    assert!(ctx.db.get_review(&job.id)?.is_none());
    let artifacts = ctx.config.state_dir().join("artifacts").join(&job.id);
    assert!(!artifacts.exists());

    // Even with its old poll time due, a terminal job is never polled again.
    ctx.make_due(&job.id, JobStatus::FailedNeedsManual)?;
    ctx.assert_ticks_leave_alone(&job.id, 3).await?;
    assert_eq!(ctx.calls(), [1, 1, 0]);
    Ok(())
}

#[tokio::test]
async fn poll_404_fails_with_invalid_token() -> Result<()> {
    let ctx = TestContext::start().await?;
    let job = ctx.submitted().await?;
    ctx.reply(
        POLL,
        Reply::fixture(StatusCode::NOT_FOUND, "error_404.json"),
    );

    assert_eq!(ctx.poll(&job.id).await?, Attempt::Ran);

    let failed = ctx.job(&job.id)?;
    assert_eq!(failed.status, JobStatus::Failed);
    assert_eq!(failed.attempt, 1);
    assert_eq!(failed.last_error.as_deref(), Some("invalid token"));
    assert_eq!(failed.next_poll_at, None);
    assert_no_lease(&failed);
    let events = ctx.events(&job.id)?;
    assert_eq!(event_types(&events).last(), Some(&"invalid_token"));
    assert_eq!(only_event(&events, "invalid_token")["token"], JOB_ID);
    ctx.assert_ticks_leave_alone(&job.id, 2).await?;
    assert_eq!(ctx.calls(), [1, 1, 0]);
    Ok(())
}

// ---------------------------------------------------------------------------------------
// Definitive refusals
// ---------------------------------------------------------------------------------------

/// The provider refused the credentials: FAILED_NEEDS_MANUAL after one POST, quoting the
/// provider, never resent, and the key appears nowhere.
async fn assert_submit_auth_refusal(ctx: &TestContext, quoted: &str) -> Result<()> {
    let job = ctx.enqueue()?;

    assert_eq!(ctx.submit(&job.id).await?, Attempt::Ran);

    let refused = ctx.job(&job.id)?;
    assert_eq!(refused.status, JobStatus::FailedNeedsManual);
    assert_eq!(refused.attempt, 1);
    assert_eq!(refused.submit_stage, None);
    assert_eq!(refused.token, None);
    assert_eq!(refused.next_poll_at, None);
    assert!(!refused.fallback_used);
    assert_no_lease(&refused);
    let last_error = diagnostic(&refused)?;
    assert_contains(
        Some(last_error),
        "authentication failed: CSPaper refused the API key",
    );
    assert_contains(Some(last_error), quoted);
    assert!(!last_error.contains(SECRET), "{last_error}");
    let events = ctx.events(&job.id)?;
    assert_eq!(
        event_types(&events),
        [
            "job_enqueued",
            "submit_dispatched",
            "submit_failed_needs_manual"
        ]
    );
    assert_eq!(events[2].payload["reason"].as_str(), Some(last_error));
    assert_eq!(ctx.mock().only(SUBMIT).api_key(), Some(SECRET));
    assert_eq!(ctx.calls(), [1, 0, 0]);

    ctx.assert_ticks_leave_alone(&job.id, 3).await?;
    assert_eq!(ctx.calls(), [1, 0, 0]);
    ctx.assert_secret_contained()
}

#[tokio::test]
async fn submit_401_needs_manual_and_is_never_resent() -> Result<()> {
    let ctx = TestContext::start().await?;
    // The key was sent; something on the way dropped it.
    ctx.reply(
        SUBMIT,
        Reply::fixture(StatusCode::UNAUTHORIZED, "error_401.json"),
    );
    assert_submit_auth_refusal(
        &ctx,
        "(401 Unauthorized): API Key is missing. Please provide it in the X-API-Key header.",
    )
    .await
}

#[tokio::test]
async fn submit_403_needs_manual_and_is_never_resent() -> Result<()> {
    let ctx = TestContext::start().await?;
    ctx.mock().accept_only(OTHER_ORG_KEY);
    assert_submit_auth_refusal(&ctx, "(403 Forbidden): Invalid API Key").await
}

/// The provider rejected the request as invalid: FAILED after exactly one POST.
async fn assert_submit_rejected(reply: Reply, details: &[&str]) -> Result<()> {
    let ctx = TestContext::start().await?;
    ctx.reply(SUBMIT, reply);
    let job = ctx.enqueue()?;

    assert_eq!(ctx.submit(&job.id).await?, Attempt::Ran);

    let failed = ctx.job(&job.id)?;
    assert_eq!(failed.status, JobStatus::Failed);
    assert_eq!(failed.attempt, 1);
    assert_eq!(failed.submit_stage, None);
    assert_eq!(failed.token, None);
    assert_eq!(failed.next_poll_at, None);
    assert_no_lease(&failed);
    let last_error = diagnostic(&failed)?;
    assert!(last_error.starts_with("request rejected: "), "{last_error}");
    for detail in details {
        assert_contains(Some(last_error), detail);
    }
    let events = ctx.events(&job.id)?;
    assert_eq!(
        event_types(&events),
        ["job_enqueued", "submit_dispatched", "submit_failed"]
    );
    assert_eq!(events[2].payload["reason"].as_str(), Some(last_error));
    assert_eq!(ctx.calls(), [1, 0, 0]);

    ctx.assert_ticks_leave_alone(&job.id, 2).await?;
    assert_eq!(ctx.calls(), [1, 0, 0]);
    Ok(())
}

#[tokio::test]
async fn submit_400_unknown_template_fails_naming_the_template() -> Result<()> {
    assert_submit_rejected(
        Reply::fixture(StatusCode::BAD_REQUEST, "error_400.json"),
        &[
            &format!("CSPaper rejected review template \"{AGENT}\" (400)"),
            "Unknown agent_id",
        ],
    )
    .await
}

#[tokio::test]
async fn submit_422_fails() -> Result<()> {
    assert_submit_rejected(
        Reply::fixture(StatusCode::UNPROCESSABLE_ENTITY, "error_422.json"),
        &[
            "CSPaper rejected the submission as incomplete or invalid (422)",
            "Field required: file",
        ],
    )
    .await
}

// ---------------------------------------------------------------------------------------
// Rate limits
// ---------------------------------------------------------------------------------------

fn too_many_requests() -> Reply {
    Reply::error(StatusCode::TOO_MANY_REQUESTS, "Too many requests")
}

impl TestContext {
    /// Claim the job for a submit the way the daemon does at `at`.
    fn claim_submit_at(&self, job_id: &str, at: DateTime<Utc>) -> Result<Option<Lease>> {
        self.db.claim_job(
            job_id,
            WorkKind::Submit,
            ClaimTiming::WhenDue,
            at,
            Duration::minutes(30),
        )
    }
}

#[tokio::test]
async fn submit_429_requeues_on_the_schedule_and_claims_respect_the_cooldown() -> Result<()> {
    let ctx = TestContext::start().await?;
    ctx.reply(SUBMIT, too_many_requests());
    let job = ctx.enqueue()?;

    assert_eq!(ctx.submit(&job.id).await?, Attempt::Ran);

    let queued = ctx.job(&job.id)?;
    assert_eq!(queued.status, JobStatus::Queued);
    assert_eq!(queued.attempt, 1);
    assert_eq!(queued.submit_stage, None);
    assert_eq!(queued.token, None);
    assert_no_lease(&queued);
    assert_contains(
        queued.last_error.as_deref(),
        "CSPaper rate limited the submission: Too many requests",
    );
    // schedule [10, 20, 40, 60], attempt 1, jitter 0 -> 20 minutes
    assert_minutes_from_now(queued.next_poll_at, 19, 20);
    let events = ctx.events(&job.id)?;
    assert_eq!(
        event_types(&events),
        ["job_enqueued", "submit_dispatched", "submit_rate_limited"]
    );
    assert_eq!(events[2].payload["retry_after_source"], "schedule");
    assert_eq!(ctx.calls(), [1, 0, 0]);

    // Ticks inside the cooldown send nothing, and nothing can claim the job early.
    ctx.assert_ticks_leave_alone(&job.id, 2).await?;
    let now = Utc::now();
    assert!(
        ctx.claim_submit_at(&job.id, now + Duration::minutes(19))?
            .is_none()
    );
    let due = ctx
        .claim_submit_at(&job.id, now + Duration::minutes(21))?
        .context("claimable after the cooldown")?;
    assert!(ctx.db.release_lease(&due)?);

    // Once due, the daemon resubmits exactly once.
    ctx.make_due(&job.id, JobStatus::Queued)?;
    ctx.reply(SUBMIT, Reply::accepted(JOB_ID));
    ctx.tick().await?;
    let submitted = ctx.job(&job.id)?;
    assert_eq!(submitted.status, JobStatus::Processing);
    assert_eq!(submitted.token.as_deref(), Some(JOB_ID));
    assert_eq!(ctx.calls(), [2, 0, 0]);
    Ok(())
}

#[tokio::test]
async fn submit_429_with_retry_after_uses_the_server_delay() -> Result<()> {
    let ctx = TestContext::start().await?;
    ctx.reply(SUBMIT, too_many_requests().header("retry-after", "120"));
    let job = ctx.enqueue()?;
    let before = Utc::now();

    assert_eq!(ctx.submit(&job.id).await?, Attempt::Ran);

    let queued = ctx.job(&job.id)?;
    assert_eq!(queued.status, JobStatus::Queued);
    assert_eq!(queued.attempt, 1);
    assert_no_lease(&queued);
    assert_seconds_after(queued.next_poll_at, before, 115, 130);
    let events = ctx.events(&job.id)?;
    assert_eq!(
        only_event(&events, "submit_rate_limited")["retry_after_source"],
        "server"
    );
    ctx.assert_ticks_leave_alone(&job.id, 1).await?;
    assert!(
        ctx.claim_submit_at(&job.id, before + Duration::seconds(60))?
            .is_none()
    );
    let due = ctx
        .claim_submit_at(&job.id, before + Duration::seconds(135))?
        .context("claimable after Retry-After")?;
    assert!(ctx.db.release_lease(&due)?);
    assert_eq!(ctx.calls(), [1, 0, 0]);
    Ok(())
}

#[tokio::test]
async fn poll_429_keeps_processing_and_reschedules() -> Result<()> {
    let ctx = TestContext::start().await?;
    let job = ctx.submitted().await?;
    ctx.reply(POLL, too_many_requests().header("retry-after", "300"));
    ctx.reply(POLL, too_many_requests());

    let before = Utc::now();
    assert_eq!(ctx.poll(&job.id).await?, Attempt::Ran);
    let limited = ctx.job(&job.id)?;
    assert_eq!(limited.status, JobStatus::Processing);
    assert_eq!(limited.attempt, 1);
    assert_eq!(limited.token.as_deref(), Some(JOB_ID));
    assert_no_lease(&limited);
    assert_contains(
        limited.last_error.as_deref(),
        "CSPaper rate limited the poll: Too many requests",
    );
    assert_seconds_after(limited.next_poll_at, before, 295, 310);

    assert_eq!(ctx.poll(&job.id).await?, Attempt::Ran);
    let limited = ctx.job(&job.id)?;
    assert_eq!(limited.status, JobStatus::Processing);
    assert_eq!(limited.attempt, 2);
    // schedule [10, 20, 40, 60], attempt 2 -> 40 minutes
    assert_minutes_from_now(limited.next_poll_at, 39, 40);

    let events = ctx.events(&job.id)?;
    let sources: Vec<&Value> = events
        .iter()
        .filter(|event| event.event_type == "poll_rate_limited")
        .map(|event| &event.payload["retry_after_source"])
        .collect();
    assert_eq!(sources, [&json!("server"), &json!("schedule")]);
    assert_eq!(ctx.calls(), [1, 2, 0]);
    Ok(())
}

// ---------------------------------------------------------------------------------------
// Unknown outcomes: park, never resend
// ---------------------------------------------------------------------------------------

/// Parked as SUBMITTED/UNCERTAIN after one POST, with the CSPaper reconcile hint, and
/// left alone by every later tick.
async fn assert_parked_without_resend(ctx: &TestContext, job: &Job, detail: &str) -> Result<()> {
    let parked = ctx.job(&job.id)?;
    assert_eq!(parked.status, JobStatus::Submitted);
    assert_eq!(parked.submit_stage, Some(SubmitStage::Uncertain));
    assert_eq!(parked.attempt, 1);
    assert_eq!(parked.token, None);
    assert_eq!(parked.next_poll_at, None);
    assert!(!parked.fallback_used);
    assert_no_lease(&parked);
    let last_error = diagnostic(&parked)?;
    assert_contains(
        Some(last_error),
        "submission outcome unknown (primary channel)",
    );
    assert_contains(Some(last_error), detail);
    for hint in CSPAPER_RECONCILE_HINTS {
        assert_contains(Some(last_error), hint);
    }
    let events = ctx.events(&job.id)?;
    assert_eq!(
        event_types(&events),
        [
            "job_enqueued",
            "submit_dispatched",
            "submit_outcome_unknown"
        ]
    );
    let unknown = &events[2].payload;
    assert_eq!(unknown["source"], "dispatch_error");
    assert_eq!(unknown["channel"], "primary");
    assert_contains(unknown["error"].as_str(), detail);
    assert_eq!(ctx.calls(), [1, 0, 0]);

    ctx.assert_ticks_leave_alone(&job.id, 3).await?;
    assert_eq!(ctx.calls(), [1, 0, 0]);
    ctx.assert_secret_contained()
}

async fn assert_reply_parks_uncertain(reply: Reply, detail: &str) -> Result<()> {
    let ctx = TestContext::start().await?;
    ctx.reply(SUBMIT, reply);
    let job = ctx.enqueue()?;
    assert_eq!(ctx.submit(&job.id).await?, Attempt::Ran);
    assert_parked_without_resend(&ctx, &job, detail).await
}

#[tokio::test]
async fn submit_500_parks_uncertain() -> Result<()> {
    assert_reply_parks_uncertain(
        Reply::error(StatusCode::INTERNAL_SERVER_ERROR, "upstream exploded"),
        "CSPaper answered 500 Internal Server Error: upstream exploded",
    )
    .await
}

#[tokio::test]
async fn submit_2xx_without_job_id_parks_uncertain() -> Result<()> {
    assert_reply_parks_uncertain(
        Reply::json(
            StatusCode::ACCEPTED,
            &json!({ "status": 200, "data": { "status": "PENDING" } }),
        ),
        "CSPaper answered 202 Accepted without a usable job_id",
    )
    .await
}

#[tokio::test]
async fn submit_2xx_html_parks_uncertain() -> Result<()> {
    assert_reply_parks_uncertain(
        Reply::html(StatusCode::OK, "<html>Sign in</html>"),
        "CSPaper answered 200 OK without a usable job_id: <html>Sign in</html>",
    )
    .await
}

#[tokio::test]
async fn submit_303_parks_uncertain() -> Result<()> {
    assert_reply_parks_uncertain(
        Reply::html(StatusCode::SEE_OTHER, "").header("location", "/platform/review"),
        "CSPaper answered 303 See Other (redirect to /platform/review)",
    )
    .await
}

/// 302 is the default post/redirect/get answer of many frameworks: the job may
/// already exist, so it is never resubmitted (unlike 307/308, which ask for it).
#[tokio::test]
async fn submit_302_parks_uncertain() -> Result<()> {
    assert_reply_parks_uncertain(
        Reply::html(StatusCode::FOUND, "").header("location", "/platform/review"),
        "CSPaper answered 302 Found (redirect to /platform/review)",
    )
    .await
}

#[tokio::test]
async fn submit_response_lost_parks_uncertain() -> Result<()> {
    let ctx = TestContext::start().await?;
    ctx.push(SUBMIT, Step::Hang);
    let job = ctx.enqueue()?;

    let worker = worker::submit_job(&ctx.config, &ctx.db, &job.id);
    tokio::pin!(worker);
    until_entered(worker.as_mut(), ctx.mock()).await?;
    // CSPaper holds the request; the connection dies before any response.
    ctx.server.cut_connections().await;
    let attempt = tokio::time::timeout(GUARD, worker)
        .await
        .context("worker did not finish")??;
    assert_eq!(attempt, Attempt::Ran);

    // "got no response" proves the send-error branch, not the 20-minute dispatch timeout.
    assert_parked_without_resend(&ctx, &job, "CSPaper submit got no response").await
}

// ---------------------------------------------------------------------------------------
// Redirects are refused: the key never follows one
// ---------------------------------------------------------------------------------------

#[tokio::test]
async fn submit_redirect_is_not_followed() -> Result<()> {
    let ctx = TestContext::start().await?;
    let target = MockServer::start().await?;
    let location = format!("{}{SUBMIT_PATH}", target.base_url);
    ctx.reply(
        SUBMIT,
        Reply::error(StatusCode::TEMPORARY_REDIRECT, "moved").header("location", &location),
    );
    let job = ctx.enqueue()?;

    assert_eq!(ctx.submit(&job.id).await?, Attempt::Ran);

    let failed = ctx.job(&job.id)?;
    assert_eq!(failed.status, JobStatus::Failed);
    assert_eq!(failed.attempt, 1);
    assert_eq!(failed.token, None);
    assert_eq!(failed.submit_stage, None);
    let last_error = diagnostic(&failed)?;
    assert_contains(
        Some(last_error),
        &format!("schema error: CSPaper answered 307 Temporary Redirect redirecting to {location}"),
    );
    assert_contains(
        Some(last_error),
        "providers.cspaper.base_url must point at the API host",
    );
    let events = ctx.events(&job.id)?;
    assert_eq!(
        event_types(&events),
        ["job_enqueued", "submit_dispatched", "submit_failed"]
    );
    assert_eq!(ctx.calls(), [1, 0, 0]);
    assert_eq!(target.state.calls(), [0, 0, 0], "the redirect was followed");
    Ok(())
}

#[tokio::test]
async fn poll_redirect_is_not_followed_and_polling_continues() -> Result<()> {
    let ctx = TestContext::start().await?;
    let job = ctx.submitted().await?;
    let target = MockServer::start().await?;
    let location = format!("{}{REVIEWS_PATH}/{JOB_ID}", target.base_url);
    ctx.reply(
        POLL,
        Reply::html(StatusCode::FOUND, "").header("location", &location),
    );

    assert_eq!(ctx.poll(&job.id).await?, Attempt::Ran);

    let polled = ctx.job(&job.id)?;
    assert_eq!(polled.status, JobStatus::Processing);
    assert_eq!(polled.attempt, 1);
    assert_eq!(polled.token.as_deref(), Some(JOB_ID));
    assert_no_lease(&polled);
    assert_minutes_from_now(polled.next_poll_at, 19, 20);
    assert_contains(polled.last_error.as_deref(), "302 Found");
    assert_contains(polled.last_error.as_deref(), "providers.cspaper.base_url");
    let events = ctx.events(&job.id)?;
    assert_eq!(event_types(&events).last(), Some(&"poll_error"));
    assert_contains(
        only_event(&events, "poll_error")["error"].as_str(),
        "providers.cspaper.base_url",
    );
    assert_eq!(ctx.calls(), [1, 1, 0]);
    assert_eq!(target.state.calls(), [0, 0, 0], "the redirect was followed");
    Ok(())
}

// ---------------------------------------------------------------------------------------
// Restart and crash recovery
// ---------------------------------------------------------------------------------------

#[tokio::test]
async fn processing_job_completes_after_a_restart_without_resubmitting() -> Result<()> {
    let ctx = TestContext::start().await?;
    let job = ctx.submitted().await?;
    ctx.make_due(&job.id, JobStatus::Processing)?;

    // The process exits: its config and database handle are gone.
    let TestContext { ctx, server } = ctx;
    let Ctx {
        tmp,
        config,
        db,
        pdf_path,
    } = ctx;
    let state_dir = config.state_dir();
    let db_path = db.path.clone();
    drop(db);
    drop(config);

    // A new process loads a fresh config and opens the same database file.
    let config = cspaper_config(&state_dir, &pdf_path, &server.base_url);
    let db = Db::new_file(db_path);
    db.ensure_schema()?;
    let ctx = TestContext {
        ctx: Ctx {
            tmp,
            config,
            db,
            pdf_path,
        },
        server,
    };
    let resumed = ctx.job(&job.id)?;
    assert_eq!(resumed.status, JobStatus::Processing);
    assert_eq!(resumed.token.as_deref(), Some(JOB_ID));
    ctx.reply(POLL, Reply::job("review_completed.json", JOB_ID));

    ctx.tick().await?;

    let done = ctx.job(&job.id)?;
    assert_eq!(done.status, JobStatus::Completed);
    assert_eq!(
        ctx.mock().only(POLL).uri,
        format!("{REVIEWS_PATH}/{JOB_ID}")
    );
    // The only POST is the one from before the restart.
    assert_eq!(ctx.calls(), [1, 1, 0]);
    let dir = ctx.config.state_dir().join("artifacts").join(&job.id);
    assert_eq!(read_json(&dir.join("meta.json"))?["token"], JOB_ID);
    ctx.assert_secret_contained()
}

/// The worker dies while the POST is in flight. Its live lease keeps every tick off the
/// job; once the lease lapses, recovery parks it as UNCERTAIN. The operator then imports
/// the CSPaper job id and polling completes the job, still without a second POST.
#[tokio::test]
async fn crash_mid_post_parks_uncertain_until_the_job_id_is_imported() -> Result<()> {
    let ctx = TestContext::start().await?;
    ctx.push(SUBMIT, Step::Gate(Reply::accepted(JOB_ID)));
    let job = ctx.enqueue()?;

    {
        let worker = worker::submit_job(&ctx.config, &ctx.db, &job.id);
        tokio::pin!(worker);
        until_entered(worker.as_mut(), ctx.mock()).await?;
    } // the worker future is dropped here: the crash

    let in_flight = ctx.job(&job.id)?;
    assert_eq!(in_flight.status, JobStatus::Submitted);
    assert_eq!(in_flight.submit_stage, Some(SubmitStage::Dispatched));
    assert!(in_flight.lease_owner.is_some());
    assert_minutes_from_now(in_flight.lease_expires_at, 29, 30);
    ctx.assert_ticks_leave_alone(&job.id, 1).await?;

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
    for hint in CSPAPER_RECONCILE_HINTS {
        assert_contains(parked.last_error.as_deref(), hint);
    }
    let events = ctx.events(&job.id)?;
    assert_eq!(
        event_types(&events),
        [
            "job_enqueued",
            "submit_dispatched",
            "submit_outcome_unknown"
        ]
    );
    assert_eq!(events[2].payload["source"], "lease_expired");
    ctx.assert_ticks_leave_alone(&job.id, 2).await?;
    assert_eq!(ctx.calls(), [1, 0, 0]);

    // Found in CSPaper's review list: attach its job id, as `import-token` does.
    ctx.db.attach_token_to_job(&job.id, JOB_ID, Utc::now())?;
    let attached = ctx.job(&job.id)?;
    assert_eq!(attached.status, JobStatus::Processing);
    assert_eq!(attached.submit_stage, None);
    ctx.reply(POLL, Reply::job("review_completed.json", JOB_ID));

    assert_eq!(ctx.poll(&job.id).await?, Attempt::Ran);

    assert_eq!(ctx.job(&job.id)?.status, JobStatus::Completed);
    assert_eq!(ctx.calls(), [1, 1, 0]);
    Ok(())
}

// ---------------------------------------------------------------------------------------
// No Stanford fallback, no request before local checks pass
// ---------------------------------------------------------------------------------------

/// A definitive failure that would hand a Stanford job to the browser fallback ends a
/// cspaper job instead; the armed fallback script never runs.
async fn assert_fallback_never_runs(ctx: &mut TestContext, detail: &str) -> Result<()> {
    let marker = ctx.arm_marker_fallback()?;
    assert_eq!(
        ctx.config.providers.stanford.fallback_mode,
        "node_playwright"
    );
    let job = ctx.enqueue()?;

    assert_eq!(ctx.submit(&job.id).await?, Attempt::Ran);

    let failed = ctx.job(&job.id)?;
    // A fallback run would end PROCESSING, or FAILED_NEEDS_MANUAL where node is missing.
    assert_eq!(failed.status, JobStatus::Failed);
    assert!(!failed.fallback_used);
    let last_error = diagnostic(&failed)?;
    assert_contains(Some(last_error), detail);
    assert!(!last_error.contains("fallback"), "{last_error}");
    let events = ctx.events(&job.id)?;
    assert_eq!(dispatch_channels(&events), ["primary"]);
    assert_eq!(
        event_types(&events),
        ["job_enqueued", "submit_dispatched", "submit_failed"]
    );
    assert!(
        !marker.exists(),
        "the Stanford fallback ran for a cspaper job"
    );
    Ok(())
}

#[tokio::test]
async fn rejected_submission_never_reaches_the_stanford_fallback() -> Result<()> {
    let mut ctx = TestContext::start().await?;
    ctx.reply(
        SUBMIT,
        Reply::fixture(StatusCode::UNPROCESSABLE_ENTITY, "error_422.json"),
    );
    assert_fallback_never_runs(&mut ctx, "Field required: file").await?;
    assert_eq!(ctx.calls(), [1, 0, 0]);
    Ok(())
}

#[tokio::test]
async fn unreachable_provider_never_reaches_the_stanford_fallback() -> Result<()> {
    let mut ctx = TestContext::start().await?;
    // Nothing listens there: the connection is refused before anything is sent.
    ctx.config.providers.cspaper.base_url = "http://127.0.0.1:9".to_string();
    assert_fallback_never_runs(&mut ctx, "network error").await?;
    assert_eq!(ctx.calls(), [0, 0, 0]);
    Ok(())
}

/// A submission refused before sending: a terminal outcome and no request at all.
async fn assert_refused_before_sending(
    ctx: &TestContext,
    job: &Job,
    status: JobStatus,
    event: &str,
    detail: &str,
) -> Result<()> {
    assert_eq!(ctx.submit(&job.id).await?, Attempt::Ran);

    let refused = ctx.job(&job.id)?;
    assert_eq!(refused.status, status);
    assert_eq!(refused.attempt, 1);
    assert_eq!(refused.submit_stage, None);
    assert_eq!(refused.token, None);
    assert_no_lease(&refused);
    assert_contains(refused.last_error.as_deref(), detail);
    let events = ctx.events(&job.id)?;
    assert_eq!(event_types(&events).last(), Some(&event));
    assert_eq!(ctx.calls(), [0, 0, 0], "a request was sent");
    ctx.assert_ticks_leave_alone(&job.id, 2).await?;
    assert_eq!(ctx.calls(), [0, 0, 0], "a tick sent a request");
    Ok(())
}

#[tokio::test]
async fn missing_api_key_needs_manual_without_any_request() -> Result<()> {
    let mut ctx = TestContext::start().await?;
    ctx.config.providers.cspaper.api_key = None;
    // The shared operations refuse to enqueue such a request at all...
    let refused = ctx.request(None).expect_err("enqueued without an API key");
    assert!(
        matches!(
            refused,
            OpError::ProviderNotConfigured {
                setting: "api_key",
                ..
            }
        ),
        "{refused:?}"
    );
    // ...but a job queued while a key was still configured reaches the worker.
    let job = ctx.insert_job(Some(AGENT))?;
    assert_refused_before_sending(
        &ctx,
        &job,
        JobStatus::FailedNeedsManual,
        "submit_failed_needs_manual",
        "authentication failed: no CSPaper API key configured",
    )
    .await
}

#[tokio::test]
async fn missing_template_fails_without_any_request() -> Result<()> {
    let ctx = TestContext::start().await?;
    let job = ctx.insert_job(None)?;
    assert_refused_before_sending(
        &ctx,
        &job,
        JobStatus::Failed,
        "submit_failed",
        "request rejected: no CSPaper review template",
    )
    .await
}

#[tokio::test]
async fn non_pdf_snapshot_fails_without_any_request() -> Result<()> {
    let ctx = TestContext::start().await?;
    fs::write(&ctx.pdf_path, b"<html>not a pdf</html>")?;
    let job = ctx.enqueue()?;
    assert_refused_before_sending(
        &ctx,
        &job,
        JobStatus::Failed,
        "submit_failed",
        "request rejected: paper.pdf is not a PDF",
    )
    .await
}

// ---------------------------------------------------------------------------------------
// End to end through the shared review operations
// ---------------------------------------------------------------------------------------

#[tokio::test]
async fn shared_operations_request_complete_replay_and_new_template_rounds() -> Result<()> {
    let mut ctx = TestContext::start().await?;

    let first = ctx.request(Some("req-1"))?;
    assert_eq!(first.disposition, RequestDisposition::Created);
    assert_eq!(first.reason, None);
    assert_eq!(first.job.backend, "cspaper");
    assert_eq!(first.job.status, JobStatus::Queued);
    assert_eq!(first.job.venue.as_deref(), Some(AGENT));
    assert_eq!(first.job.review_options, desk_rejection("true"));
    assert_eq!(first.job.round_no, 1);
    assert_eq!(first.input.venue.as_deref(), Some(AGENT));
    let job_id = first.job.job_id.clone();

    ctx.reply(SUBMIT, Reply::accepted(JOB_ID));
    assert_eq!(ctx.submit(&job_id).await?, Attempt::Ran);
    ctx.reply(POLL, Reply::job("review_completed.json", JOB_ID));
    assert_eq!(ctx.poll(&job_id).await?, Attempt::Ran);

    let view = ctx.ops().get_job(&job_id)?;
    assert_eq!(view.status, JobStatus::Completed);
    assert!(view.has_token);
    assert!(view.review_available);
    let review = ctx.ops().get_review(&ReviewQuery {
        job_id: job_id.clone(),
        part: ReviewPart::Summary,
    })?;
    assert_eq!(review.score.as_deref(), Some("7.5"));
    assert_eq!(review.title.as_deref(), Some(TITLE));
    let review_md = review.artifacts.review_md.context("archived review.md")?;
    assert!(Path::new(&review_md).exists());
    assert_eq!(ctx.calls(), [1, 1, 0]);

    // Replaying the request returns the same job and sends nothing.
    let replay = ctx.request(Some("req-1"))?;
    assert_eq!(replay.disposition, RequestDisposition::Existing);
    assert_eq!(replay.reason, Some(ExistingReason::RequestReplay));
    assert_eq!(replay.job.job_id, job_id);
    assert_eq!(replay.job.status, JobStatus::Completed);
    assert_eq!(ctx.calls(), [1, 1, 0]);

    // Another template is another review: the old key does not fit it...
    ctx.config.providers.cspaper.agent_id = Some(OTHER_AGENT.to_string());
    let conflict = ctx
        .request(Some("req-1"))
        .expect_err("a replayed key with another template");
    assert!(
        matches!(conflict, OpError::RequestConflict(_)),
        "{conflict:?}"
    );
    // ...and a new request opens round 2, submitted with the new template.
    let second = ctx.request(Some("req-2"))?;
    assert_eq!(second.disposition, RequestDisposition::Created);
    assert_ne!(second.job.job_id, job_id);
    assert_eq!(second.job.venue.as_deref(), Some(OTHER_AGENT));
    assert_eq!(second.job.version_no, first.job.version_no);
    assert_eq!(second.job.round_no, 2);
    ctx.reply(SUBMIT, Reply::accepted("cspaper-round-2"));
    assert_eq!(ctx.submit(&second.job.job_id).await?, Attempt::Ran);
    assert_eq!(
        ctx.job(&second.job.job_id)?.token.as_deref(),
        Some("cspaper-round-2")
    );

    // So does a different desk-rejection setting.
    ctx.config.providers.cspaper.desk_rejection_enabled = false;
    let third = ctx.request(Some("req-3"))?;
    assert_eq!(third.disposition, RequestDisposition::Created);
    assert_eq!(third.job.review_options, desk_rejection("false"));
    assert_eq!(third.job.round_no, 3);
    ctx.reply(SUBMIT, Reply::accepted("cspaper-round-3"));
    assert_eq!(ctx.submit(&third.job.job_id).await?, Attempt::Ran);
    assert_eq!(ctx.job(&third.job.job_id)?.status, JobStatus::Processing);

    let sent: Vec<(Option<String>, Option<String>)> = ctx
        .mock()
        .received(SUBMIT)
        .iter()
        .map(|post| (post.text("agent_id"), post.text(DESK_REJECTION)))
        .collect();
    let expected = [
        (AGENT, "true"),
        (OTHER_AGENT, "true"),
        (OTHER_AGENT, "false"),
    ]
    .map(|(agent, desk)| (Some(agent.to_string()), Some(desk.to_string())));
    assert_eq!(sent, expected);
    assert_eq!(ctx.calls(), [3, 1, 0]);
    ctx.assert_secret_contained()
}

// ---------------------------------------------------------------------------------------
// Secret containment
// ---------------------------------------------------------------------------------------

/// The scanner behind every containment check: a key stored in a row, or in any file of
/// the state dir, is caught.
#[tokio::test]
async fn secret_scan_catches_a_stored_key() -> Result<()> {
    let caught = |ctx: &TestContext| {
        std::panic::catch_unwind(AssertUnwindSafe(|| assert_secret_absent(&ctx.ctx, SECRET)))
            .is_err()
    };

    let ctx = TestContext::start().await?;
    let job = ctx.enqueue()?;
    ctx.assert_secret_contained()?;
    ctx.db.add_event(
        Some(PROJECT),
        Some(&job.id),
        "leak",
        json!({ "header": SECRET }),
    )?;
    assert!(caught(&ctx), "a key in an event row went unnoticed");

    let ctx = TestContext::start().await?;
    ctx.enqueue()?;
    fs::write(
        ctx.config.state_dir().join("artifacts.log"),
        format!("X-API-Key: {SECRET}"),
    )?;
    assert!(caught(&ctx), "a key in a state file went unnoticed");
    Ok(())
}

#[tokio::test]
async fn a_key_echoed_by_the_provider_on_submit_is_scrubbed() -> Result<()> {
    let ctx = TestContext::start().await?;
    ctx.mock().accept_only(OTHER_ORG_KEY);
    ctx.mock().echo_refused_key();
    let job = ctx.enqueue()?;

    assert_eq!(ctx.submit(&job.id).await?, Attempt::Ran);

    let refused = ctx.job(&job.id)?;
    assert_eq!(refused.status, JobStatus::FailedNeedsManual);
    assert_contains(
        refused.last_error.as_deref(),
        "(403 Forbidden): Invalid API Key: [redacted]",
    );
    // The provider really did quote the key it was sent.
    assert_eq!(ctx.mock().only(SUBMIT).api_key(), Some(SECRET));
    ctx.assert_secret_contained()
}

#[tokio::test]
async fn a_key_echoed_by_the_provider_on_poll_is_scrubbed() -> Result<()> {
    let ctx = TestContext::start().await?;
    let job = ctx.submitted().await?;
    ctx.mock().accept_only(OTHER_ORG_KEY);
    ctx.mock().echo_refused_key();

    assert_eq!(ctx.poll(&job.id).await?, Attempt::Ran);

    // A refused poll keeps the job; a later one succeeds once the key is fixed.
    let polled = ctx.job(&job.id)?;
    assert_eq!(polled.status, JobStatus::Processing);
    assert_eq!(polled.attempt, 1);
    assert_contains(
        polled.last_error.as_deref(),
        "authentication failed: CSPaper refused the API key (403 Forbidden): Invalid API Key: [redacted]",
    );
    let events = ctx.events(&job.id)?;
    assert_contains(
        only_event(&events, "poll_error")["error"].as_str(),
        "[redacted]",
    );
    assert_eq!(ctx.calls(), [1, 1, 0]);
    ctx.assert_secret_contained()
}
