//! Personal CSPaper web sessions. Contract evidence: docs/providers/cspaper-web.md.
//! Login is independent of review choices; jobs pin choices and the web transport.

use super::{
    BackendError, ReviewFetchResult, SubmitReceipt, SubmitRequest, SubmitStep, parse_retry_after,
};
use crate::config::{Config, CspaperModel, CspaperProviderConfig, Redacted};
use crate::http::Redirects;
use reqwest::{
    Response, StatusCode,
    header::{COOKIE, HeaderValue},
    multipart,
};
use reqwest_middleware::ClientWithMiddleware;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub const TOKEN_PREFIX: &str = "web:";
const LOGIN_HINT: &str =
    "CSPaper session missing or expired; run reviewloop cspaper login --cookie-file <path>";

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WebAgent {
    pub id: String,
    pub name: String,
    pub abbreviation: String,
    #[serde(default)]
    pub year: Value,
    #[serde(default)]
    pub track: Value,
    #[serde(rename = "supportsNoDra")]
    pub supports_no_dra: bool,
    pub llm: Vec<CatalogModel>,
}

/// The website passes the selected catalogue object as llmOptions, including
/// provider metadata. Only vendor/model are persisted as the job's choices.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CatalogModel {
    pub vendor: String,
    pub model: String,
    #[serde(flatten)]
    pub metadata: serde_json::Map<String, Value>,
}

impl WebAgent {
    pub fn select_model(&self, query: Option<&str>) -> Result<CspaperModel, BackendError> {
        let matches: Vec<&CatalogModel> = self
            .llm
            .iter()
            .filter(|model| {
                query.is_none_or(|q| {
                    q == model.model || q == format!("{}/{}", model.vendor, model.model)
                })
            })
            .collect();
        match matches.as_slice() {
            [model] => Ok(CspaperModel { vendor: model.vendor.clone(), model: model.model.clone() }),
            _ => Err(BackendError::Rejected("select one available model with --model vendor/model; run reviewloop cspaper agents".into())),
        }
    }
}

pub fn select_agent<'a>(agents: &'a [WebAgent], query: &str) -> Result<&'a WebAgent, BackendError> {
    if let Some(agent) = agents.iter().find(|agent| agent.id == query) {
        return Ok(agent);
    }
    let matches: Vec<&WebAgent> = agents
        .iter()
        .filter(|agent| {
            agent.name.eq_ignore_ascii_case(query) || agent.abbreviation.eq_ignore_ascii_case(query)
        })
        .collect();
    match matches.as_slice() {
        [agent] => Ok(agent),
        _ => Err(BackendError::Rejected("template name is unknown or ambiguous; run reviewloop cspaper agents and select a unique name or id".into())),
    }
}

/// No Debug: credentials must never enter diagnostics.
pub struct CspaperWeb {
    client: ClientWithMiddleware,
    base_url: String,
    cookie: Option<Redacted<String>>,
}

impl CspaperWeb {
    pub fn new(config: &CspaperProviderConfig, client: ClientWithMiddleware) -> Self {
        Self {
            client,
            base_url: config.base_url.trim_end_matches('/').into(),
            cookie: config.cookie.clone(),
        }
    }

    pub fn from_config(config: &Config) -> anyhow::Result<Self> {
        Ok(Self::new(
            &config.providers.cspaper,
            crate::http::build_client(config, None, None, Redirects::Refuse)?,
        ))
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    fn cookie_header(&self) -> Result<HeaderValue, BackendError> {
        let cookie = self
            .cookie
            .as_ref()
            .ok_or_else(|| BackendError::Auth(LOGIN_HINT.into()))?;
        let cookie = normalize_cookie(cookie)?;
        let mut header = HeaderValue::from_str(&cookie)
            .map_err(|_| BackendError::Auth("invalid CSPaper Cookie header".into()))?;
        header.set_sensitive(true);
        Ok(header)
    }

    /// A read-only authenticated list call; no template/model or API key needed.
    pub async fn check_session(&self) -> Result<(), BackendError> {
        let payload = self
            .get(
                "/api/review/list?page_num=0&page_size=1&sort_by=date&sort_direction=desc",
                true,
            )
            .await?;
        if payload
            .pointer("/data/data")
            .and_then(Value::as_array)
            .is_none()
        {
            return Err(BackendError::Schema(
                "CSPaper session check returned no review list".into(),
            ));
        }
        Ok(())
    }

    /// Public catalogue. Portal-only and API-only templates aren't personal reviews.
    pub async fn agents(&self) -> Result<Vec<WebAgent>, BackendError> {
        let payload = self.get("/api/conference", false).await?;
        let rows = payload
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                BackendError::Schema("CSPaper catalogue returned no data array".into())
            })?;
        rows.iter()
            .filter(|row| {
                row.pointer("/access/user").and_then(Value::as_bool) == Some(true)
                    && row.get("portal").is_none_or(Value::is_null)
            })
            .map(|row| {
                serde_json::from_value(row.clone()).map_err(|_| {
                    BackendError::Schema(
                        "CSPaper catalogue has an invalid template or model".into(),
                    )
                })
            })
            .collect()
    }

    async fn get(&self, path: &str, authenticated: bool) -> Result<Value, BackendError> {
        let mut request = self.client.get(self.endpoint(path));
        if authenticated {
            request = request.header(COOKIE, self.cookie_header()?);
        }
        let response = request.send().await.map_err(|_| {
            BackendError::Network("CSPaper web request failed before a response was read".into())
        })?;
        self.read_response(response, false).await
    }

    /// Never echo response text, headers or redirects in errors. Some gateways
    /// include request credentials in their diagnostic bodies.
    async fn read_response(
        &self,
        response: Response,
        submitting: bool,
    ) -> Result<Value, BackendError> {
        let status = response.status();
        let retry_after = parse_retry_after(response.headers());
        let classify = |code: u16| -> BackendError {
            match code {
                401 | 403 => BackendError::Auth(LOGIN_HINT.into()),
                429 => BackendError::RateLimited {
                    message: "CSPaper web rate limit".into(),
                    retry_after,
                },
                500..=599 if !submitting => BackendError::Server {
                    status: code,
                    body: "CSPaper web server error".into(),
                },
                300..=399 if !submitting => BackendError::Auth(LOGIN_HINT.into()),
                400..=499 => BackendError::Rejected(format!(
                    "CSPaper web request rejected (HTTP {code}); check the template/model and account on cspaper.org"
                )),
                _ if submitting => unknown_outcome(),
                _ => BackendError::Schema("unexpected CSPaper web response".into()),
            }
        };
        if !status.is_success() {
            return Err(classify(status.as_u16()));
        }
        let payload = response.json::<Value>().await.map_err(|_| {
            if submitting { unknown_outcome() } else { BackendError::Auth("CSPaper returned no JSON; check the session with reviewloop cspaper auth-status".into()) }
        })?;
        match payload.get("status").and_then(Value::as_u64) {
            Some(200) => {}
            Some(code) if code <= u16::MAX as u64 => return Err(classify(code as u16)),
            _ if submitting => return Err(unknown_outcome()),
            _ => {
                return Err(BackendError::Schema(
                    "CSPaper web envelope has no valid status".into(),
                ));
            }
        }
        Ok(payload)
    }

    fn scrub(&self, payload: &mut Value) {
        let Some(cookie) = &self.cookie else {
            return;
        };
        let mut secrets: Vec<&str> = cookie
            .split(';')
            .filter_map(|pair| {
                let (name, value) = pair.trim().split_once('=')?;
                let name = name.to_ascii_lowercase();
                // Short preferences such as locale=en aren't credentials and
                // would otherwise corrupt ordinary review text and JSON keys.
                let credential = ["session", "token", "auth", "csrf"]
                    .iter()
                    .any(|part| name.contains(part));
                (credential || value.len() >= 16).then_some(value)
            })
            .filter(|value| !value.is_empty())
            .collect();
        secrets.push(cookie.as_str());
        scrub_value(payload, &secrets);
    }

    pub async fn submit(&self, req: SubmitRequest) -> Result<SubmitReceipt, BackendError> {
        let cookie = self.cookie_header()?;
        let agents = self.agents().await?;
        let agent = agents.iter().find(|agent| Some(agent.id.as_str()) == req.venue.as_deref())
            .ok_or_else(|| BackendError::Rejected("recorded web template is unavailable; select a template and request a new review".into()))?;
        let model = agent.llm.iter().find(|model| {
            Some(model.vendor.as_str()) == req.review_options.get("model_vendor")
                && Some(model.model.as_str()) == req.review_options.get("model")
        }).ok_or_else(|| BackendError::Rejected("recorded web model is unavailable for this template; select a model and request a new review".into()))?;
        let desk =
            match req
                .review_options
                .get(super::cspaper::DESK_REJECTION_ENABLED)
            {
                Some("true") => true,
                Some("false") if agent.supports_no_dra => false,
                _ => return Err(BackendError::Rejected(
                    "invalid desk rejection choice, or the template does not support disabling it"
                        .into(),
                )),
            };
        let bytes = tokio::fs::read(&req.pdf_path)
            .await
            .map_err(|_| BackendError::Rejected("could not read the pinned PDF".into()))?;
        if !bytes.starts_with(b"%PDF-") {
            return Err(BackendError::Rejected(
                "CSPaper accepts PDF files only".into(),
            ));
        }
        let file = multipart::Part::bytes(bytes)
            .file_name("paper.pdf")
            .mime_str("application/pdf")
            .map_err(|_| BackendError::Rejected("invalid PDF content type".into()))?;
        let form = multipart::Form::new()
            .part("file", file)
            .text(
                "review_request",
                json!({"conferenceId":agent.id,"deskRejectionEnabled":desk}).to_string(),
            )
            .text(
                "llmOptions",
                serde_json::to_string(model)
                    .map_err(|_| BackendError::Schema("invalid model options".into()))?,
            );
        req.progress.enter(SubmitStep::Confirm);
        // Multipart cannot be cloned: the proxy middleware never repeats this POST.
        let response = self
            .client
            .post(self.endpoint("/api/review/register/pdf"))
            .header(COOKIE, cookie)
            .header("Origin", &self.base_url)
            .header("Referer", self.endpoint("/review"))
            .multipart(form)
            .send()
            .await
            .map_err(|error| {
                if error.is_connect() {
                    BackendError::Network(
                        "could not connect to CSPaper web; no submission sent".into(),
                    )
                } else {
                    unknown_outcome()
                }
            })?;
        let payload = self.read_response(response, true).await?;
        let id = payload
            .pointer("/data/id")
            .and_then(Value::as_str)
            .filter(|id| valid_id(id))
            .ok_or_else(unknown_outcome)?;
        Ok(SubmitReceipt {
            token: format!("{TOKEN_PREFIX}{id}"),
        })
    }

    pub async fn fetch_review(&self, id: &str) -> Result<ReviewFetchResult, BackendError> {
        if !valid_id(id) {
            return Ok(ReviewFetchResult::InvalidToken);
        }
        let response = self
            .client
            .get(self.endpoint(&format!("/api/review/{id}")))
            .header(COOKIE, self.cookie_header()?)
            .send()
            .await
            .map_err(|_| BackendError::Network("CSPaper web poll failed".into()))?;
        if matches!(response.status(), StatusCode::NOT_FOUND | StatusCode::GONE) {
            return Ok(ReviewFetchResult::InvalidToken);
        }
        let payload = self.read_response(response, false).await?;
        let data = &payload["data"];
        if data.get("id").and_then(Value::as_str) != Some(id) {
            return Err(BackendError::Schema(
                "CSPaper returned a different review id".into(),
            ));
        }
        match data.get("status").and_then(Value::as_str) {
            Some("pending" | "progressing") => Ok(ReviewFetchResult::Processing),
            Some("failed") => Ok(ReviewFetchResult::Failed { reason: "CSPaper web review failed; inspect it on cspaper.org before requesting a new review".into() }),
            Some("completed") => {
                let content = data.get("result").and_then(Value::as_str).filter(|text| !text.trim().is_empty())
                    .ok_or_else(|| BackendError::Schema("completed CSPaper web review has no result".into()))?;
                let mut raw_json = json!({
                    "provider":"cspaper", "transport":"web", "provider_job_id":id,
                    "title":data.pointer("/paperMetadata/title"), "venue":data.get("templateId"),
                    "content":content, "result_summary":data.get("resultSummary"),
                    "desk_rejected":data.pointer("/resultSummary/deskReject"),
                    "numerical_score":data.pointer("/resultSummary/mainScore").filter(|score| score.is_number()),
                    "provider_raw":payload,
                });
                // Classify the protocol before scrubbing: a non-auth Cookie such
                // as locale=en must not corrupt a receipt or the pending status.
                self.scrub(&mut raw_json);
                Ok(ReviewFetchResult::Ready { raw_json })
            }
            _ => Err(BackendError::Schema("unknown CSPaper web review status".into())),
        }
    }
}

/// A file may contain a raw Cookie value or a single `Cookie:` header line.
/// Browser database access and browser session extraction are not involved.
pub fn normalize_cookie(raw: &str) -> Result<String, BackendError> {
    let line = raw.trim();
    let cookie = if line
        .get(..7)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("cookie:"))
    {
        line[7..].trim()
    } else {
        line
    };
    if cookie.is_empty()
        || cookie.bytes().any(|byte| byte < 32 || byte == 127)
        || cookie.split(';').any(|pair| {
            pair.trim()
                .split_once('=')
                .is_none_or(|(name, _)| name.is_empty() || name.chars().any(char::is_whitespace))
        })
    {
        return Err(BackendError::Auth(
            "expected a single Cookie header value in the file, without other headers".into(),
        ));
    }
    HeaderValue::from_str(cookie)
        .map_err(|_| BackendError::Auth("invalid CSPaper Cookie header".into()))?;
    Ok(cookie.into())
}

fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

fn unknown_outcome() -> BackendError {
    BackendError::OutcomeUnknown("CSPaper may have accepted the PDF; check https://cspaper.org/reviews and import web:<review-id>; do not submit again automatically".into())
}

fn scrub_value(value: &mut Value, secrets: &[&str]) {
    match value {
        Value::String(text) => {
            for secret in secrets {
                *text = text.replace(secret, "[redacted]");
            }
        }
        Value::Array(items) => {
            for item in items {
                scrub_value(item, secrets);
            }
        }
        Value::Object(items) => {
            let original = std::mem::take(items);
            for (mut key, mut item) in original {
                for secret in secrets {
                    key = key.replace(secret, "[redacted]");
                }
                scrub_value(&mut item, secrets);
                items.insert(key, item);
            }
        }
        _ => {}
    }
}
