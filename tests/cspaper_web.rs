//! Cookie transport contract tests. Synthetic responses match the read-only
//! live schema; tests never upload a real paper or access a browser session.
mod common;

use anyhow::{Context, Result};
use axum::{
    Router,
    body::to_bytes,
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
};
use reviewloop::{
    application::{Approval, RequestOrigin, ReviewOps, ReviewRequest},
    backend::{
        BackendError, ReviewBackend, ReviewFetchResult, SubmitProgress, SubmitRequest,
        cspaper::CspaperBackend,
        cspaper_web::{CspaperWeb, normalize_cookie, select_agent},
    },
    config::{Config, CspaperModel, GlobalConfigFile, ProjectConfigFile, Redacted},
    model::{JobStatus, SubmitStage},
    worker,
};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    fs,
    process::Command,
    sync::{Arc, Mutex},
};
use tokio::{net::TcpListener, task::JoinHandle};

const COOKIE: &str = "session=secret_session_value; csrf=secret_csrf_value; locale=en";
const AGENT: &str = "ARD-Review_method_2026_1";
const ID: &str = "d7634e85-2459-457a-bbc9-cfa48456f2aa";

fn catalogue() -> Value {
    json!({"status":200,"data":[{
        "id":AGENT,"name":"Accurate, Reasonable and Diverse AI Peer Review","abbreviation":"ARD-Review",
        "access":{"user":true,"api":false},"portal":null,"supportsNoDra":false,
        "llm":[{"vendor":"zai","model":"glm-test","errorRate":0.1}]
    },{"access":{"user":false},"id":"api-only"},{"access":{"user":true},"portal":{},"id":"portal-only"}]})
}

#[derive(Clone)]
struct Reply {
    path: String,
    code: StatusCode,
    body: String,
}
impl Reply {
    fn json(path: &str, body: Value) -> Self {
        Self {
            path: path.into(),
            code: StatusCode::OK,
            body: body.to_string(),
        }
    }
}
struct Received {
    path: String,
    headers: HeaderMap,
    body: String,
}
#[derive(Default)]
struct ServerState {
    replies: VecDeque<Reply>,
    received: Vec<Received>,
}
struct Mock {
    url: String,
    state: Arc<Mutex<ServerState>>,
    task: JoinHandle<()>,
}
impl Drop for Mock {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Mock {
    async fn start(replies: Vec<Reply>) -> Result<Self> {
        let state = Arc::new(Mutex::new(ServerState {
            replies: replies.into(),
            received: vec![],
        }));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let app = Router::new().fallback(any(serve)).with_state(state.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Ok(Self { url, state, task })
    }
    fn configure(&self, config: &mut Config) {
        config.providers.cspaper.base_url = self.url.clone();
        config.providers.cspaper.cookie = Some(Redacted(COOKIE.into()));
        config.providers.cspaper.agent_id = Some(AGENT.into());
        config.providers.cspaper.model = Some(CspaperModel {
            vendor: "zai".into(),
            model: "glm-test".into(),
        });
    }
}
async fn serve(State(state): State<Arc<Mutex<ServerState>>>, req: Request) -> Response {
    let path = req.uri().path().to_string();
    let headers = req.headers().clone();
    let body =
        String::from_utf8(to_bytes(req.into_body(), 1_000_000).await.unwrap().to_vec()).unwrap();
    let mut state = state.lock().unwrap();
    state.received.push(Received {
        path: path.clone(),
        headers,
        body,
    });
    let reply = state
        .replies
        .pop_front()
        .expect("unexpected request, possibly a retry or wrong transport");
    assert_eq!(path, reply.path);
    (
        reply.code,
        [
            ("content-type", "application/json"),
            ("location", "/unexpected-redirect-target"),
        ],
        reply.body,
    )
        .into_response()
}
fn request(ctx: &common::Ctx) -> SubmitRequest {
    SubmitRequest {
        pdf_path: ctx.pdf_path.clone(),
        email: String::new(),
        venue: Some(AGENT.into()),
        review_options: ctx.config.review_options_for(&ctx.config.papers[0]),
        progress: SubmitProgress::default(),
    }
}
fn web_paper(ctx: &mut common::Ctx, mock: &Mock) {
    mock.configure(&mut ctx.config);
    ctx.config.papers[0].backend = "cspaper".into();
}

#[tokio::test]
async fn cookie_login_needs_no_template_model_or_api_key() -> Result<()> {
    let mock = Mock::start(vec![Reply::json(
        "/api/review/list",
        json!({"status":200,"data":{"data":[],"pagination":{"hasNextPage":false}}}),
    )])
    .await?;
    let mut config = Config::default();
    config.providers.cspaper.base_url = mock.url.clone();
    config.providers.cspaper.cookie = Some(Redacted(COOKIE.into()));
    CspaperWeb::from_config(&config)?.check_session().await?;
    let state = mock.state.lock().unwrap();
    assert_eq!(state.received[0].headers["cookie"], COOKIE);
    assert!(!state.received[0].headers.contains_key("x-api-key"));
    Ok(())
}

#[tokio::test]
async fn submit_uses_browser_multipart_and_pinned_choices_then_resumes_by_web_receipt() -> Result<()>
{
    let mut replies = vec![
        Reply::json("/api/conference", catalogue()),
        Reply::json(
            "/api/review/register/pdf",
            json!({"status":200,"data":{"id":ID}}),
        ),
    ];
    for status in ["pending", "progressing", "completed"] {
        replies.push(Reply::json(&format!("/api/review/{ID}"), json!({"status":200,"data":{
            "id":ID,"status":status,"templateId":AGENT,"result":"# Review\nSynthetic result with secret_session_value echoed.",
            "paperMetadata":{"title":"Synthetic paper"},"resultSummary":{"deskReject":false,"mainScore":6.5,"mainScoreNorm":0.61}
        }})));
    }
    let mock = Mock::start(replies).await?;
    let mut ctx = common::Ctx::new()?;
    web_paper(&mut ctx, &mock);
    ctx.config.providers.cspaper.api_key = Some(Redacted("unused_organization_key".into()));
    let job = ReviewOps::new(&ctx.config, &ctx.db)
        .request_review(&ReviewRequest {
            paper_id: common::PAPER.into(),
            request_key: Some("web-round-1".into()),
            force: false,
            approval: Approval::Granted,
            origin: RequestOrigin::Agent,
        })?
        .job;
    worker::submit_job(&ctx.config, &ctx.db, &job.job_id).await?;
    let saved = ctx.db.get_job(&job.job_id)?.context("job")?;
    assert_eq!(saved.token.as_deref(), Some(format!("web:{ID}").as_str()));
    assert_eq!(saved.status, JobStatus::Processing);
    // Changing the template/model for new jobs does not affect polling this one.
    ctx.config.providers.cspaper.agent_id = None;
    ctx.config.providers.cspaper.model = None;
    for _ in 0..3 {
        worker::poll_job(&ctx.config, &ctx.db, &job.job_id).await?;
    }
    let done = ctx.db.get_job(&job.job_id)?.context("done")?;
    assert_eq!(done.status, JobStatus::Completed);
    let artifact_dir = ctx.config.state_dir().join("artifacts").join(&job.job_id);
    let raw: Value = serde_json::from_str(&fs::read_to_string(artifact_dir.join("review.json"))?)?;
    assert_eq!(raw["numerical_score"], json!(6.5));
    assert!(
        raw["content"]
            .as_str()
            .context("content survives preference-cookie redaction")?
            .contains("Synthetic result")
    );
    let markdown = fs::read_to_string(artifact_dir.join("review.md"))?;
    assert!(markdown.contains("Provider Score"));
    assert!(!markdown.contains("ICLR-calibrated"));
    let state = mock.state.lock().unwrap();
    let posted = &state.received[1];
    assert!(posted.body.contains("name=\"review_request\""));
    assert!(
        posted
            .body
            .contains(&format!("\"conferenceId\":\"{AGENT}\""))
    );
    assert!(posted.body.contains("\"deskRejectionEnabled\":true"));
    assert!(posted.body.contains("name=\"llmOptions\""));
    assert!(posted.body.contains("\"model\":\"glm-test\""));
    assert!(posted.body.contains(&fs::read_to_string(&ctx.pdf_path)?));
    assert!(!posted.body.contains("agent_id"));
    assert!(!posted.body.contains("secret_session_value"));
    for call in &state.received {
        assert!(!call.headers.contains_key("x-api-key"));
        if call.path != "/api/conference" {
            assert_eq!(call.headers["cookie"], COOKIE);
        } else {
            assert!(!call.headers.contains_key("cookie"));
        }
    }
    common::assert_secret_absent(&ctx, "secret_session_value")?;
    common::assert_secret_absent(&ctx, "secret_csrf_value")?;
    Ok(())
}

#[tokio::test]
async fn uncertain_posts_park_without_resubmission_or_organization_fallback() -> Result<()> {
    for (status, body) in [
        (StatusCode::INTERNAL_SERVER_ERROR, "secret_session_value"),
        (StatusCode::FOUND, "login"),
        (StatusCode::OK, "<html>login</html>"),
        (StatusCode::OK, "{\"status\":200,\"data\":{}}"),
    ] {
        let mock = Mock::start(vec![
            Reply::json("/api/conference", catalogue()),
            Reply {
                path: "/api/review/register/pdf".into(),
                code: status,
                body: body.into(),
            },
        ])
        .await?;
        let mut ctx = common::Ctx::new()?;
        web_paper(&mut ctx, &mock);
        let job = ReviewOps::new(&ctx.config, &ctx.db)
            .request_review(&ReviewRequest {
                paper_id: common::PAPER.into(),
                request_key: None,
                force: false,
                approval: Approval::Granted,
                origin: RequestOrigin::Submit,
            })?
            .job;
        worker::submit_job(&ctx.config, &ctx.db, &job.job_id).await?;
        worker::submit_job(&ctx.config, &ctx.db, &job.job_id).await?;
        let saved = ctx.db.get_job(&job.job_id)?.context("job")?;
        assert_eq!(saved.status, JobStatus::Submitted);
        assert_eq!(saved.submit_stage, Some(SubmitStage::Uncertain));
        assert_eq!(mock.state.lock().unwrap().received.len(), 2);
        common::assert_secret_absent(&ctx, "secret_session_value")?;
    }
    Ok(())
}

#[tokio::test]
async fn expired_sessions_and_redirects_do_not_leak_response_bodies() -> Result<()> {
    for (code, body) in [
        (StatusCode::UNAUTHORIZED, "secret_session_value"),
        (StatusCode::FORBIDDEN, "secret_csrf_value"),
        (StatusCode::FOUND, "login"),
        (StatusCode::OK, "<html>secret_session_value</html>"),
    ] {
        let mock = Mock::start(vec![Reply {
            path: "/api/review/list".into(),
            code,
            body: body.into(),
        }])
        .await?;
        let mut config = Config::default();
        mock.configure(&mut config);
        let error = CspaperWeb::from_config(&config)?
            .check_session()
            .await
            .unwrap_err();
        assert!(matches!(error, BackendError::Auth(_)));
        assert!(!error.to_string().contains("secret_"));
        assert_eq!(mock.state.lock().unwrap().received.len(), 1);
    }
    Ok(())
}

#[tokio::test]
async fn malformed_completion_wrong_id_and_unknown_status_are_not_archived() -> Result<()> {
    for data in [
        json!({"id":ID,"status":"completed","result":""}),
        json!({"id":"wrong","status":"completed","result":"text"}),
        json!({"id":ID,"status":"new-state"}),
    ] {
        let mock = Mock::start(vec![Reply::json(
            &format!("/api/review/{ID}"),
            json!({"status":200,"data":data}),
        )])
        .await?;
        let mut config = Config::default();
        mock.configure(&mut config);
        assert!(matches!(
            CspaperBackend::from_config(&config)?
                .fetch_review(&format!("web:{ID}"))
                .await,
            Err(BackendError::Schema(_))
        ));
    }
    Ok(())
}

#[tokio::test]
async fn catalogue_and_model_selection_accept_names_but_refuse_ambiguity() -> Result<()> {
    let mock = Mock::start(vec![Reply::json("/api/conference", catalogue())]).await?;
    let mut config = Config::default();
    mock.configure(&mut config);
    let agents = CspaperWeb::from_config(&config)?.agents().await?;
    assert_eq!(agents.len(), 1);
    let agent = select_agent(&agents, "ard-review")?;
    assert_eq!(agent.select_model(None)?.model, "glm-test");
    assert!(agent.select_model(Some("unavailable")).is_err());
    let duplicates = vec![agent.clone(), agent.clone()];
    assert!(select_agent(&duplicates, "ARD-Review").is_err());
    Ok(())
}

#[tokio::test]
async fn web_receipt_requires_cookie_and_never_uses_api_key() -> Result<()> {
    let config = Config::default();
    let error = CspaperBackend::from_config(&config)?
        .fetch_review(&format!("web:{ID}"))
        .await
        .unwrap_err();
    assert!(matches!(error, BackendError::Auth(_)));
    assert!(error.to_string().contains("cookie-file"));
    Ok(())
}

#[tokio::test]
async fn invalid_pinned_choices_never_post_and_failed_reviews_are_terminal() -> Result<()> {
    let mock = Mock::start(vec![
        Reply::json("/api/conference", catalogue()),
        Reply::json(
            &format!("/api/review/{ID}"),
            json!({"status":200,"data":{"id":ID,"status":"failed"}}),
        ),
    ])
    .await?;
    let mut ctx = common::Ctx::new()?;
    web_paper(&mut ctx, &mock);
    let mut req = request(&ctx);
    req.review_options = req.review_options.with("model", "unavailable-model");
    let backend = CspaperBackend::from_config(&ctx.config)?;
    assert!(matches!(
        backend.submit(req).await,
        Err(BackendError::Rejected(_))
    ));
    assert!(matches!(
        backend.fetch_review(&format!("web:{ID}")).await?,
        ReviewFetchResult::Failed { .. }
    ));
    assert_eq!(mock.state.lock().unwrap().received.len(), 2);
    Ok(())
}

#[test]
fn cookies_are_machine_only_and_model_transport_affect_request_identity() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("config.toml");
    let mut global = GlobalConfigFile::default();
    global.providers.cspaper.cookie = Some(Redacted(COOKIE.into()));
    global.save(&path)?;
    assert_eq!(
        GlobalConfigFile::load(&path)?.providers.cspaper.cookie,
        global.providers.cspaper.cookie
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(fs::metadata(&path)?.permissions().mode() & 0o777, 0o600);
    }
    assert!(!format!("{global:?}").contains("secret_session_value"));
    let mut config = Config::default();
    let paper = reviewloop::config::PaperConfig {
        id: "p".into(),
        pdf_path: "paper.pdf".into(),
        backend: "cspaper".into(),
        venue: Some(AGENT.into()),
    };
    let platform_options = config.review_options_for(&paper);
    config.providers.cspaper.cookie = Some(Redacted(COOKIE.into()));
    config.providers.cspaper.model = Some(CspaperModel {
        vendor: "zai".into(),
        model: "one".into(),
    });
    let web_options = config.review_options_for(&paper);
    assert_ne!(platform_options, web_options);
    config.providers.cspaper.model.as_mut().unwrap().model = "two".into();
    assert_ne!(web_options, config.review_options_for(&paper));
    assert!(!serde_json::to_string(&config.providers)?.contains("secret_session_value"));
    fs::write(
        &path,
        format!("project_id = 'p'\n[providers.cspaper]\ncookie = '{COOKIE}'\n"),
    )?;
    let error = ProjectConfigFile::load(&path).unwrap_err().to_string();
    assert!(error.contains("Cookie"));
    assert!(!error.contains("secret_session_value"));
    assert_eq!(normalize_cookie(&format!("Cookie: {COOKIE}\n"))?, COOKIE);
    assert!(normalize_cookie("session=value\nX-Header: injected").is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn login_cli_saves_only_validated_cookie_without_review_choices() -> Result<()> {
    for code in [StatusCode::OK, StatusCode::UNAUTHORIZED] {
        let mock = Mock::start(vec![Reply {
            path: "/api/review/list".into(),
            code,
            body: json!({"status":200,"data":{"data":[]}}).to_string(),
        }])
        .await?;
        let tmp = tempfile::tempdir()?;
        let root = tmp.path().join("reviewloop");
        fs::create_dir(&root)?;
        let path = root.join("config.toml");
        let mut global = GlobalConfigFile::default();
        global.providers.cspaper.base_url = mock.url.clone();
        global.save(&path)?;
        let cookie_path = tmp.path().join("cookie.txt");
        fs::write(&cookie_path, COOKIE)?;
        let output = Command::new(env!("CARGO_BIN_EXE_reviewloop"))
            .args(["cspaper", "login", "--cookie-file"])
            .arg(&cookie_path)
            .env("XDG_CONFIG_HOME", tmp.path())
            .env("HOME", tmp.path())
            .env_remove("REVIEWLOOP_CSPAPER_COOKIE")
            .env_remove("REVIEWLOOP_CSPAPER_API_KEY")
            .output()?;
        assert_eq!(
            output.status.success(),
            code == StatusCode::OK,
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let saved = GlobalConfigFile::load(&path)?;
        assert_eq!(
            saved.providers.cspaper.cookie.is_some(),
            code == StatusCode::OK
        );
        assert!(saved.providers.cspaper.agent_id.is_none());
        assert!(saved.providers.cspaper.api_key.is_none());
        assert!(!String::from_utf8_lossy(&output.stdout).contains("secret_"));
        assert!(!String::from_utf8_lossy(&output.stderr).contains("secret_"));
    }
    Ok(())
}
