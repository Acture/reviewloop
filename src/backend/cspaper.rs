//! CSPaper Agentic Review (<https://cspaper.org/platform/review>).
//!
//! Contract, checked on 2026-10-09 against the live service and the official
//! examples (<https://github.com/cspaper/platform-examples>). The examples'
//! README also names `/api/v1/platform/...`; on the live service those paths
//! redirect to a sign-in page, so only `/api/platform/...` is used.
//!
//! - `POST /api/platform/review`, multipart `agent_id` (review template),
//!   `file` (PDF) and `desk_rejection_enabled` (`"true"`/`"false"`), answers
//!   `202 {"status":200,"data":{"job_id":..,"status":"PENDING"}}`. The request
//!   creates the job in one step and carries no idempotency key.
//! - `GET /api/platform/reviews/{job_id}` answers `{"status":200,"data":{..}}`
//!   whose `status` moves PENDING → PROCESSING → COMPLETED or FAILED. A
//!   completed job holds a markdown `result` and a JSON-encoded
//!   `result_summary`; a failed one a `failed_reason`.
//! - Errors use `{"status":N,"data":{"message":..}}`: 400 unknown `agent_id`,
//!   401 missing key, 403 invalid key, 404 no such job for this organisation,
//!   422 missing `file` or `agent_id`.
//!
//! The organisation API key travels only in the `X-API-Key` header of each
//! request, never in a URL, error message, job field or archive. The job id
//! becomes the job's token: the provider reference polling resumes from after
//! a restart.

use super::{
    BackendError, ReviewBackend, ReviewFetchResult, SubmitReceipt, SubmitRequest, parse_retry_after,
};
use crate::config::{CSPAPER_API_KEY_ENV, Config, CspaperProviderConfig, Redacted};
use crate::http::{Redirects, describe_error};
use crate::model::{ProviderUsage, ReviewOptions};
use async_trait::async_trait;
use chrono::{DateTime, Datelike, TimeZone, Utc};
use reqwest::header::{HeaderMap, HeaderValue, LOCATION};
use reqwest::{Response, StatusCode, multipart};
use reqwest_middleware::ClientWithMiddleware;
use serde::Serialize;
use serde_json::{Map, Value, json};
use std::collections::HashSet;

pub const BACKEND: &str = "cspaper";
pub const PROVIDER_NAME: &str = "CSPaper Agentic Review";
/// Review option carrying the desk-rejection setting; see
/// [`crate::config::Config::review_options_for`].
pub const DESK_REJECTION_ENABLED: &str = "desk_rejection_enabled";

const SUBMIT_PATH: &str = "/api/platform/review";
const REVIEWS_PATH: &str = "/api/platform/reviews";
const API_KEY_HEADER: &str = "X-API-Key";
/// Provider text quoted into job errors and events is cut to this many chars.
const MAX_QUOTED_CHARS: usize = 512;
/// Retrying a job resends what it recorded at request time, so a corrected
/// template needs a new request.
const NEW_REVIEW_HINT: &str = "request a new review with `reviewloop submit --paper-id <paper>` (retrying this job resends what it recorded)";

/// No `Debug`: it would be one `{:?}` away from printing the key holder.
pub struct CspaperBackend {
    client: ClientWithMiddleware,
    base_url: String,
    api_key: Option<Redacted<String>>,
}

impl CspaperBackend {
    pub fn new(config: &CspaperProviderConfig, client: ClientWithMiddleware) -> Self {
        Self {
            client,
            base_url: config.base_url.trim_end_matches('/').to_string(),
            api_key: config.api_key.clone().filter(|key| !key.trim().is_empty()),
        }
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    /// Checked before anything is sent, so a missing key never reaches the
    /// provider and never consumes a submission.
    fn api_key_header(&self) -> Result<HeaderValue, BackendError> {
        let key = self.api_key.as_ref().ok_or_else(|| {
            BackendError::Auth(format!(
                "no CSPaper API key configured; set providers.cspaper.api_key in the global config (~/.config/reviewloop/config.toml) or {CSPAPER_API_KEY_ENV}"
            ))
        })?;
        let mut value = HeaderValue::from_str(key).map_err(|_| {
            BackendError::Auth("the configured CSPaper API key is not a valid header value".into())
        })?;
        value.set_sensitive(true);
        Ok(value)
    }

    /// The provider's own message when the body is its error envelope,
    /// otherwise the body itself; shortened, and with the key scrubbed in case
    /// a server ever echoes it.
    fn quote(&self, body: &str) -> String {
        let text = serde_json::from_str::<Value>(body)
            .ok()
            .and_then(|value| envelope_message(&value))
            .unwrap_or_else(|| body.trim().to_string());
        let text = match &self.api_key {
            Some(key) if !key.is_empty() => text.replace(key.as_str(), "[redacted]"),
            _ => text,
        };
        let mut chars = text.chars();
        let mut quoted: String = chars.by_ref().take(MAX_QUOTED_CHARS).collect();
        if chars.next().is_some() {
            quoted.push('…');
        }
        if quoted.is_empty() {
            "<empty body>".to_string()
        } else {
            quoted
        }
    }

    fn auth_error(&self, status: StatusCode, body: &str) -> BackendError {
        BackendError::Auth(format!(
            "CSPaper refused the API key ({status}): {}; check providers.cspaper.api_key or {CSPAPER_API_KEY_ENV}",
            self.quote(body)
        ))
    }
}

#[async_trait]
impl ReviewBackend for CspaperBackend {
    fn name(&self) -> &'static str {
        BACKEND
    }

    async fn submit(&self, req: SubmitRequest) -> Result<SubmitReceipt, BackendError> {
        // Everything that can be refused locally is refused before sending.
        let agent_id = req
            .venue
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                BackendError::Rejected(format!(
                    "no CSPaper review template recorded on this job; set providers.cspaper.agent_id or the paper's venue to an agent_id such as ICLR_main_2026_1, then {NEW_REVIEW_HINT}"
                ))
            })?
            .to_string();
        let desk_rejection_enabled = desk_rejection_field(&req.review_options)?;
        let api_key = self.api_key_header()?;
        let file_name = req
            .pdf_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| BackendError::Rejected("invalid PDF filename".to_string()))?
            .to_string();
        let bytes = tokio::fs::read(&req.pdf_path)
            .await
            .map_err(|e| BackendError::Network(format!("failed to read PDF: {e}")))?;
        if !bytes.starts_with(b"%PDF-") {
            return Err(BackendError::Rejected(format!(
                "{file_name} is not a PDF; CSPaper reviews PDF files only"
            )));
        }

        let file = multipart::Part::bytes(bytes)
            .file_name(file_name)
            .mime_str("application/pdf")
            .map_err(|e| BackendError::Rejected(format!("invalid mime: {e}")))?;
        // A multipart body is streamed, so the proxy middleware never re-sends
        // this request: the provider sees at most one copy of it.
        let form = multipart::Form::new()
            .text("agent_id", agent_id.clone())
            .text(DESK_REJECTION_ENABLED, desk_rejection_enabled)
            .part("file", file);

        let resp = self
            .client
            .post(self.endpoint(SUBMIT_PATH))
            .header(API_KEY_HEADER, api_key)
            .multipart(form)
            .send()
            .await
            .map_err(|e| {
                // Only a failed connect proves nothing reached the provider.
                // Described without the URL, as for every request (see `describe_error`).
                let cause = describe_error(&e, e.url());
                if e.is_connect() {
                    BackendError::Network(cause)
                } else {
                    BackendError::OutcomeUnknown(format!("CSPaper submit got no response: {cause}"))
                }
            })?;

        let status = resp.status();
        let (retry_after, location) = response_meta(resp.headers());
        let body = match resp.text().await {
            Ok(text) => text,
            Err(e) if status.is_success() => {
                return Err(BackendError::OutcomeUnknown(format!(
                    "CSPaper answered {status} but its receipt was unreadable: {e}"
                )));
            }
            Err(_) => String::new(),
        };

        match status {
            status if status.is_success() => parse_receipt(&body).ok_or_else(|| {
                BackendError::OutcomeUnknown(format!(
                    "CSPaper answered {status} without a usable job_id: {}",
                    self.quote(&body)
                ))
            }),
            // Assumes CSPaper throttles before creating a job; its documentation
            // does not say. A resend after the cooldown relies on that.
            StatusCode::TOO_MANY_REQUESTS => Err(BackendError::RateLimited {
                message: format!("CSPaper rate limited the submission: {}", self.quote(&body)),
                retry_after,
            }),
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Err(self.auth_error(status, &body)),
            StatusCode::BAD_REQUEST => Err(BackendError::Rejected(format!(
                "CSPaper rejected review template {agent_id:?} (400): {}; fix providers.cspaper.agent_id or the paper's venue, then {NEW_REVIEW_HINT}",
                self.quote(&body)
            ))),
            StatusCode::UNPROCESSABLE_ENTITY => Err(BackendError::Rejected(format!(
                "CSPaper rejected the submission as incomplete or invalid (422): {}",
                self.quote(&body)
            ))),
            // A server may answer a POST it has processed with 301/302/303
            // (post/redirect/get); only 307/308 ask for the request to be resent.
            StatusCode::MOVED_PERMANENTLY | StatusCode::FOUND | StatusCode::SEE_OTHER => {
                Err(BackendError::OutcomeUnknown(format!(
                    "CSPaper answered {status} (redirect to {}) instead of a receipt",
                    location.as_deref().unwrap_or("<none>")
                )))
            }
            status if status.is_redirection() => Err(redirect_error(status, location.as_deref())),
            status if status.is_server_error() => Err(BackendError::OutcomeUnknown(format!(
                "CSPaper answered {status}: {}",
                self.quote(&body)
            ))),
            status if status.is_client_error() => Err(BackendError::Rejected(format!(
                "CSPaper rejected the submission ({status}): {}",
                self.quote(&body)
            ))),
            status => Err(BackendError::OutcomeUnknown(format!(
                "CSPaper answered {status} instead of a receipt: {}",
                self.quote(&body)
            ))),
        }
    }

    async fn fetch_review(&self, token: &str) -> Result<ReviewFetchResult, BackendError> {
        // The job id becomes a URL path segment; anything else (e.g. a mistyped
        // `import-token`) cannot name a CSPaper job.
        if !is_job_id(token) {
            return Ok(ReviewFetchResult::InvalidToken);
        }
        let api_key = self.api_key_header()?;
        let resp = self
            .client
            .get(self.endpoint(&format!("{REVIEWS_PATH}/{token}")))
            .header(API_KEY_HEADER, api_key)
            .send()
            .await
            // The URL holds the job id, the job's token: describe the error without it.
            .map_err(|e| BackendError::Network(describe_error(&e, e.url())))?;

        let status = resp.status();
        let (retry_after, location) = response_meta(resp.headers());
        if !status.is_success() {
            let body = read_body(resp).await;
            return match status {
                StatusCode::TOO_MANY_REQUESTS => Err(BackendError::RateLimited {
                    message: format!("CSPaper rate limited the poll: {}", self.quote(&body)),
                    retry_after,
                }),
                StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                    Err(self.auth_error(status, &body))
                }
                // Unknown job, or one owned by another organisation's key.
                StatusCode::NOT_FOUND | StatusCode::GONE => Ok(ReviewFetchResult::InvalidToken),
                status if status.is_server_error() => Err(BackendError::Server {
                    status: status.as_u16(),
                    body: self.quote(&body),
                }),
                status if status.is_redirection() => {
                    Err(redirect_error(status, location.as_deref()))
                }
                status => Err(BackendError::Schema(format!(
                    "unexpected status {status} when fetching CSPaper job {token}: {}",
                    self.quote(&body)
                ))),
            };
        }

        let payload = resp
            .json::<Value>()
            .await
            .map_err(|e| BackendError::Schema(format!("invalid CSPaper job payload: {e}")))?;
        interpret_job(token, payload)
    }
}

/// Page size for the organisation job list: CSPaper's documented default.
const LIST_PAGE_SIZE: usize = 50;
/// Pages read for one usage report at most (10 000 jobs).
const MAX_LIST_PAGES: usize = 200;

/// A CSPaper billing period. CSPaper invoices platform API keys monthly by
/// usage ("Platform APIs", cspaper.org, July 2026); months are taken in UTC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BillingMonth {
    year: i32,
    month: u32,
}

impl BillingMonth {
    pub fn containing(at: DateTime<Utc>) -> Self {
        Self {
            year: at.year(),
            month: at.month(),
        }
    }

    /// Exactly `YYYY-MM`.
    pub fn parse(raw: &str) -> Option<Self> {
        let (year, month) = raw.trim().split_once('-')?;
        let digits =
            |part: &str, len: usize| part.len() == len && part.bytes().all(|b| b.is_ascii_digit());
        if !digits(year, 4) || !digits(month, 2) {
            return None;
        }
        let month = Self {
            year: year.parse().ok()?,
            month: month.parse().ok()?,
        };
        month.start_opt().map(|_| month)
    }

    fn start_opt(self) -> Option<DateTime<Utc>> {
        Utc.with_ymd_and_hms(self.year, self.month, 1, 0, 0, 0)
            .single()
    }

    pub fn start(self) -> DateTime<Utc> {
        self.start_opt()
            .expect("a parsed or derived month has a first day")
    }

    fn next(self) -> Self {
        if self.month == 12 {
            Self {
                year: self.year + 1,
                month: 1,
            }
        } else {
            Self {
                year: self.year,
                month: self.month + 1,
            }
        }
    }

    /// `[start, end)` of the month.
    pub fn range(self) -> (DateTime<Utc>, DateTime<Utc>) {
        (self.start(), self.next().start())
    }

    pub fn contains(self, at: DateTime<Utc>) -> bool {
        let (start, end) = self.range();
        start <= at && at < end
    }
}

impl std::fmt::Display for BillingMonth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:04}-{:02}", self.year, self.month)
    }
}

/// One job from CSPaper's organisation-scoped job list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteJob {
    pub id: String,
    pub status: String,
    /// When CSPaper created the job; `None` when the entry has no parseable
    /// `created`.
    pub created: Option<DateTime<Utc>>,
}

/// The organisation's job list as far as it was read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteJobList {
    pub jobs: Vec<RemoteJob>,
    /// The list may hold more than was read: paging stopped at
    /// [`MAX_LIST_PAGES`], or the server repeated a page.
    pub incomplete: bool,
}

impl CspaperBackend {
    /// The adapter for read-only calls made outside the worker.
    pub fn from_config(config: &Config) -> anyhow::Result<Self> {
        let client = crate::http::build_client(config, None, None, Redirects::Refuse)?;
        Ok(Self::new(&config.providers.cspaper, client))
    }

    /// Every job of the key's organisation, from any client (reviewloop, the
    /// web playground, other tools). Read-only: listing is not usage.
    pub async fn list_jobs(&self) -> Result<RemoteJobList, BackendError> {
        let api_key = self.api_key_header()?;
        let mut seen = HashSet::new();
        let mut jobs = Vec::new();
        for page in 0..MAX_LIST_PAGES {
            let offset = page * LIST_PAGE_SIZE;
            let resp = self
                .client
                .get(self.endpoint(&format!(
                    "{REVIEWS_PATH}?limit={LIST_PAGE_SIZE}&offset={offset}"
                )))
                .header(API_KEY_HEADER, api_key.clone())
                .send()
                .await
                .map_err(|e| BackendError::Network(describe_error(&e, e.url())))?;
            let status = resp.status();
            let (retry_after, location) = response_meta(resp.headers());
            if !status.is_success() {
                let body = read_body(resp).await;
                return Err(match status {
                    StatusCode::TOO_MANY_REQUESTS => BackendError::RateLimited {
                        message: format!(
                            "CSPaper rate limited the job list: {}",
                            self.quote(&body)
                        ),
                        retry_after,
                    },
                    StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                        self.auth_error(status, &body)
                    }
                    status if status.is_server_error() => BackendError::Server {
                        status: status.as_u16(),
                        body: self.quote(&body),
                    },
                    status if status.is_redirection() => {
                        redirect_error(status, location.as_deref())
                    }
                    status => BackendError::Schema(format!(
                        "unexpected status {status} when listing CSPaper jobs: {}",
                        self.quote(&body)
                    )),
                });
            }
            let payload = resp
                .json::<Value>()
                .await
                .map_err(|e| BackendError::Schema(format!("invalid CSPaper job list: {e}")))?;
            let batch = parse_job_list(&payload)?;
            let received = batch.len();
            let before = jobs.len();
            jobs.extend(batch.into_iter().filter(|job| seen.insert(job.id.clone())));
            if received < LIST_PAGE_SIZE {
                return Ok(RemoteJobList {
                    jobs,
                    incomplete: false,
                });
            }
            // A full page with nothing new: the server ignores `offset`, so
            // the rest of the list cannot be read.
            if jobs.len() == before {
                return Ok(RemoteJobList {
                    jobs,
                    incomplete: true,
                });
            }
        }
        Ok(RemoteJobList {
            jobs,
            incomplete: true,
        })
    }
}

fn parse_job_list(payload: &Value) -> Result<Vec<RemoteJob>, BackendError> {
    let items = payload
        .get("data")
        .and_then(Value::as_array)
        .ok_or_else(|| BackendError::Schema("CSPaper job list has no data array".into()))?;
    items
        .iter()
        .map(|item| {
            let field = |name: &str| {
                item.get(name)
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .ok_or_else(|| {
                        BackendError::Schema(format!("CSPaper job list entry has no {name}"))
                    })
            };
            Ok(RemoteJob {
                id: field("id")?,
                status: field("status")?.trim().to_ascii_uppercase(),
                created: item
                    .get("created")
                    .and_then(Value::as_str)
                    .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
                    .map(|at| at.with_timezone(&Utc)),
            })
        })
        .collect()
}

/// CSPaper reviews of the organisation, split by status and by whether this
/// machine's reviewloop tracks them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct OrgUsage {
    pub total: u64,
    pub completed: u64,
    pub in_progress: u64,
    pub failed: u64,
    /// Statuses this adapter does not know.
    pub other: u64,
    /// Jobs whose id is the token of a reviewloop job on this machine: ones
    /// it submitted, and ones attached with `import-token`.
    pub via_reviewloop: u64,
}

impl OrgUsage {
    pub fn via_other_clients(&self) -> u64 {
        self.total - self.via_reviewloop
    }
}

/// The organisation's usage in `month` and overall.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct OrgUsageReport {
    #[serde(serialize_with = "month_str")]
    pub month: BillingMonth,
    pub in_month: OrgUsage,
    pub all_time: OrgUsage,
    /// Jobs without a creation date: in `all_time` only.
    pub undated: u64,
    pub list_incomplete: bool,
}

fn month_str<S: serde::Serializer>(month: &BillingMonth, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&month.to_string())
}

pub fn summarize(
    listed: &RemoteJobList,
    local_job_ids: &HashSet<String>,
    month: BillingMonth,
) -> OrgUsageReport {
    let count = |usage: &mut OrgUsage, job: &RemoteJob| {
        usage.total += 1;
        match job.status.as_str() {
            "COMPLETED" => usage.completed += 1,
            "PENDING" | "PROCESSING" => usage.in_progress += 1,
            "FAILED" => usage.failed += 1,
            _ => usage.other += 1,
        }
        if local_job_ids.contains(&job.id) {
            usage.via_reviewloop += 1;
        }
    };
    let mut report = OrgUsageReport {
        month,
        in_month: OrgUsage::default(),
        all_time: OrgUsage::default(),
        undated: 0,
        list_incomplete: listed.incomplete,
    };
    for job in &listed.jobs {
        count(&mut report.all_time, job);
        match job.created {
            Some(created) if month.contains(created) => count(&mut report.in_month, job),
            Some(_) => {}
            None => report.undated += 1,
        }
    }
    report
}

fn status_breakdown(usage: &OrgUsage) -> String {
    let mut statuses = vec![
        format!("{} completed", usage.completed),
        format!("{} in progress", usage.in_progress),
    ];
    if usage.failed > 0 {
        statuses.push(format!("{} failed", usage.failed));
    }
    if usage.other > 0 {
        statuses.push(format!("{} other", usage.other));
    }
    statuses.join(", ")
}

/// Reviews left in `month` under a monthly allowance, negative when over.
pub fn allowance_left(report: &OrgUsageReport, allowance: Option<u64>) -> Option<i64> {
    allowance.map(|allowance| allowance as i64 - report.in_month.total as i64)
}

/// The `reviewloop cspaper usage` report. CSPaper publishes no balance or
/// quota for API keys, so what is left comes from the monthly allowance set
/// in `providers.cspaper.monthly_allowance`.
pub fn org_usage_report(report: &OrgUsageReport, allowance: Option<u64>) -> String {
    let month = &report.in_month;
    let left = match (allowance, allowance_left(report, allowance)) {
        (Some(allowance), Some(left)) if left >= 0 => {
            format!("monthly allowance {allowance}, {left} left")
        }
        (Some(allowance), Some(left)) => {
            format!("monthly allowance {allowance}, {} over", -left)
        }
        _ => "no monthly allowance set (providers.cspaper.monthly_allowance in the global config, e.g. the volume agreed with CSPaper)".to_string(),
    };
    let mut text = format!(
        "CSPaper organisation reviews in {} (UTC), all clients: {} ({})\n  via reviewloop on this machine (submitted or imported): {}\n  via other clients: {}\n  {left}\nAll months: {} reviews, {} via reviewloop on this machine",
        report.month,
        month.total,
        status_breakdown(month),
        month.via_reviewloop,
        month.via_other_clients(),
        report.all_time.total,
        report.all_time.via_reviewloop,
    );
    if report.undated > 0 {
        text.push_str(&format!(
            "\n({} job(s) without a creation date are counted in all months only)",
            report.undated
        ));
    }
    if report.list_incomplete {
        text.push_str("\n(CSPaper's job list could not be read to the end; the counts may be low)");
    }
    text
}

/// One-line local usage summary for the CLI: what this machine's reviewloop
/// has had accepted in `month`, and uncertain submissions that may count too.
pub fn usage_note(usage: ProviderUsage, month: BillingMonth) -> String {
    let mut note = format!(
        "CSPaper reviews from this machine in {month}: {} completed, {} in progress",
        usage.completed, usage.in_progress
    );
    if usage.ended > 0 {
        note.push_str(&format!(", {} ended without a review", usage.ended));
    }
    if usage.uncertain > 0 {
        note.push_str(&format!(
            "; {} uncertain submission(s) may also count",
            usage.uncertain
        ));
    }
    note
}

fn response_meta(headers: &HeaderMap) -> (Option<chrono::Duration>, Option<String>) {
    let location = headers
        .get(LOCATION)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    (parse_retry_after(headers), location)
}

async fn read_body(resp: Response) -> String {
    resp.text().await.unwrap_or_default()
}

/// Redirects are never followed (they would carry the key elsewhere), and the
/// API itself does not redirect: one means the base URL or path is wrong.
fn redirect_error(status: StatusCode, location: Option<&str>) -> BackendError {
    BackendError::Schema(format!(
        "CSPaper answered {status} redirecting to {}; providers.cspaper.base_url must point at the API host (https://cspaper.org)",
        location.unwrap_or("<none>")
    ))
}

fn envelope_message(value: &Value) -> Option<String> {
    ["/data/message", "/message", "/detail"]
        .into_iter()
        .find_map(|pointer| value.pointer(pointer)?.as_str())
        .map(str::trim)
        .filter(|message| !message.is_empty())
        .map(str::to_string)
}

fn desk_rejection_field(options: &ReviewOptions) -> Result<String, BackendError> {
    match options.get(DESK_REJECTION_ENABLED) {
        None => Ok("true".to_string()),
        Some(value @ ("true" | "false")) => Ok(value.to_string()),
        Some(other) => Err(BackendError::Rejected(format!(
            "review option {DESK_REJECTION_ENABLED} must be true or false, got {other:?}"
        ))),
    }
}

fn is_job_id(token: &str) -> bool {
    (1..=128).contains(&token.len())
        && token
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

fn parse_receipt(body: &str) -> Option<SubmitReceipt> {
    let value: Value = serde_json::from_str(body).ok()?;
    let job_id = value.pointer("/data/job_id")?.as_str()?.trim();
    is_job_id(job_id).then(|| SubmitReceipt {
        token: job_id.to_string(),
    })
}

/// Map a job detail payload to a poll outcome.
fn interpret_job(token: &str, payload: Value) -> Result<ReviewFetchResult, BackendError> {
    let data = payload
        .get("data")
        .filter(|data| data.is_object())
        .ok_or_else(|| BackendError::Schema("CSPaper job payload has no data object".into()))?;
    if let Some(id) = data.get("id").and_then(Value::as_str)
        && id != token
    {
        return Err(BackendError::Schema(format!(
            "CSPaper returned job {id} when asked for {token}"
        )));
    }
    let status = data
        .get("status")
        .and_then(Value::as_str)
        .map(|status| status.trim().to_ascii_uppercase())
        .ok_or_else(|| BackendError::Schema("CSPaper job payload has no status".into()))?;
    match status.as_str() {
        "PENDING" | "PROCESSING" => Ok(ReviewFetchResult::Processing),
        "COMPLETED" => {
            normalize_review(token, &payload).map(|raw_json| ReviewFetchResult::Ready { raw_json })
        }
        "FAILED" => Ok(ReviewFetchResult::Failed {
            reason: data
                .get("failed_reason")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|reason| !reason.is_empty())
                .unwrap_or("CSPaper gave no reason")
                .to_string(),
        }),
        other => Err(BackendError::Schema(format!(
            "unknown CSPaper job status {other:?}"
        ))),
    }
}

/// The archived review: the provider's response verbatim under
/// `provider_raw`, plus the generic fields (`title`, `venue`,
/// `numerical_score`, `content`) the summary renderer and score readers use.
fn normalize_review(token: &str, payload: &Value) -> Result<Value, BackendError> {
    let data = &payload["data"];
    let content = data
        .get("result")
        .and_then(Value::as_str)
        .filter(|result| !result.trim().is_empty())
        .ok_or_else(|| {
            BackendError::Schema("CSPaper reported COMPLETED without a result".into())
        })?;

    let mut review = Map::new();
    review.insert("provider".into(), json!(BACKEND));
    review.insert("provider_job_id".into(), json!(token));
    for (key, pointer) in [
        ("title", "/paper_meta/title"),
        ("venue", "/agent_id"),
        ("agent_id", "/agent_id"),
        ("finished_at", "/finished_at"),
    ] {
        if let Some(value) = data.pointer(pointer).filter(|value| !value.is_null()) {
            review.insert(key.into(), value.clone());
        }
    }

    // `result_summary` arrives as a JSON-encoded string whose keys depend on
    // the template; keep it whole, decoded when possible.
    match data.get("result_summary") {
        Some(Value::String(raw)) if !raw.trim().is_empty() => {
            match serde_json::from_str::<Value>(raw) {
                Ok(summary) => insert_summary(&mut review, summary),
                Err(e) => {
                    review.insert("result_summary".into(), json!(raw));
                    review.insert("result_summary_parse_error".into(), json!(e.to_string()));
                }
            }
        }
        Some(summary @ Value::Object(_)) => insert_summary(&mut review, summary.clone()),
        _ => {}
    }

    review.insert("content".into(), json!(content));
    review.insert("provider_raw".into(), payload.clone());
    Ok(Value::Object(review))
}

fn insert_summary(review: &mut Map<String, Value>, summary: Value) {
    let score = ["overall_score", "mainScoreNorm"]
        .into_iter()
        .find_map(|key| summary.get(key).filter(|value| value.is_number()));
    if let Some(score) = score {
        review.insert("numerical_score".into(), score.clone());
    }
    if let Some(desk_reject) = summary.get("deskReject").and_then(Value::as_bool) {
        review.insert("desk_reject".into(), json!(desk_reject));
    }
    review.insert("result_summary".into(), summary);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(name: &str) -> Value {
        let path = format!(
            "{}/tests/fixtures/cspaper/{name}",
            env!("CARGO_MANIFEST_DIR")
        );
        let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
        serde_json::from_str(&text).unwrap_or_else(|e| panic!("{path}: {e}"))
    }

    const JOB: &str = "856f388c-d5cd-4409-b3bf-3e0c8279dc49";

    #[test]
    fn receipt_yields_the_job_id() {
        let body = fixture("submit_accepted.json").to_string();
        assert_eq!(parse_receipt(&body).unwrap().token, JOB);
    }

    #[test]
    fn receipt_without_a_usable_job_id_is_rejected() {
        for body in [
            r#"{"status":200,"data":{"status":"PENDING"}}"#,
            r#"{"status":200,"data":{"job_id":""}}"#,
            r#"{"status":200,"data":{"job_id":"../reviews"}}"#,
            r#"{"status":200,"data":{"job_id":42}}"#,
            "<html>sign in</html>",
        ] {
            assert!(parse_receipt(body).is_none(), "{body}");
        }
    }

    #[test]
    fn pending_and_processing_keep_polling() {
        for name in ["review_pending.json", "review_processing.json"] {
            assert!(matches!(
                interpret_job(JOB, fixture(name)),
                Ok(ReviewFetchResult::Processing)
            ));
        }
    }

    #[test]
    fn completed_review_is_normalized_and_kept_verbatim() {
        let payload = fixture("review_completed.json");
        let Ok(ReviewFetchResult::Ready { raw_json }) = interpret_job(JOB, payload.clone()) else {
            panic!("expected a ready review");
        };
        assert_eq!(raw_json["provider"], json!("cspaper"));
        assert_eq!(raw_json["provider_job_id"], json!(JOB));
        assert_eq!(raw_json["venue"], json!("ICLR_main_2026_1"));
        assert_eq!(raw_json["agent_id"], json!("ICLR_main_2026_1"));
        assert_eq!(raw_json["title"], json!("A Study of Agentic Peer Review"));
        assert_eq!(raw_json["numerical_score"], json!(7.5));
        assert_eq!(raw_json["desk_reject"], json!(false));
        assert_eq!(raw_json["result_summary"]["overall_score"], json!(7.5));
        assert!(
            raw_json["content"]
                .as_str()
                .unwrap()
                .starts_with("# Evaluation Report")
        );
        assert_eq!(raw_json["provider_raw"], payload);
    }

    #[test]
    fn main_score_norm_stands_in_for_a_missing_overall_score() {
        let mut payload = fixture("review_completed.json");
        payload["data"]["result_summary"] = json!(r#"{"mainScoreNorm": 0.62, "deskReject": true}"#);
        let Ok(ReviewFetchResult::Ready { raw_json }) = interpret_job(JOB, payload) else {
            panic!("expected a ready review");
        };
        assert_eq!(raw_json["numerical_score"], json!(0.62));
        assert_eq!(raw_json["desk_reject"], json!(true));
    }

    #[test]
    fn unparseable_summary_is_kept_raw_without_a_score() {
        let mut payload = fixture("review_completed.json");
        payload["data"]["result_summary"] = json!("{not json");
        let Ok(ReviewFetchResult::Ready { raw_json }) = interpret_job(JOB, payload) else {
            panic!("expected a ready review");
        };
        assert_eq!(raw_json["result_summary"], json!("{not json"));
        assert!(raw_json.get("result_summary_parse_error").is_some());
        assert!(raw_json.get("numerical_score").is_none());
    }

    #[test]
    fn summary_renders_the_markdown_result() {
        let Ok(ReviewFetchResult::Ready { raw_json }) =
            interpret_job(JOB, fixture("review_completed.json"))
        else {
            panic!("expected a ready review");
        };
        let markdown = crate::artifact::render_summary_markdown(&raw_json);
        assert!(
            markdown.contains("A Study of Agentic Peer Review"),
            "{markdown}"
        );
        assert!(markdown.contains("ICLR_main_2026_1"), "{markdown}");
        assert!(markdown.contains("7.5"), "{markdown}");
        assert!(markdown.contains("## Strengths"), "{markdown}");
        assert!(!markdown.contains("Raw JSON"), "{markdown}");
    }

    #[test]
    fn failed_job_carries_the_provider_reason() {
        let Ok(ReviewFetchResult::Failed { reason }) =
            interpret_job(JOB, fixture("review_failed.json"))
        else {
            panic!("expected a failed job");
        };
        assert_eq!(reason, "LLM resource exhausted");

        let mut silent = fixture("review_failed.json");
        silent["data"]["failed_reason"] = Value::Null;
        let Ok(ReviewFetchResult::Failed { reason }) = interpret_job(JOB, silent) else {
            panic!("expected a failed job");
        };
        assert_eq!(reason, "CSPaper gave no reason");
    }

    #[test]
    fn schema_drift_is_reported_not_guessed() {
        let mut no_result = fixture("review_completed.json");
        no_result["data"]["result"] = json!("  ");
        let mut unknown_status = fixture("review_pending.json");
        unknown_status["data"]["status"] = json!("ARCHIVED");
        let mut other_job = fixture("review_pending.json");
        other_job["data"]["id"] = json!("another-job");
        for (case, payload) in [
            ("no result", no_result),
            ("unknown status", unknown_status),
            ("other job", other_job),
            ("no data", json!({"status": 200})),
            ("no status", json!({"status": 200, "data": {"id": JOB}})),
        ] {
            assert!(
                matches!(interpret_job(JOB, payload), Err(BackendError::Schema(_))),
                "{case}"
            );
        }
    }

    fn month(raw: &str) -> BillingMonth {
        BillingMonth::parse(raw).expect("valid month")
    }

    fn at(raw: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(raw)
            .expect("valid timestamp")
            .with_timezone(&Utc)
    }

    #[test]
    fn usage_note_splits_reviews_by_stage_and_flags_uncertain_ones() {
        let quiet = usage_note(
            ProviderUsage {
                completed: 3,
                in_progress: 1,
                ..ProviderUsage::default()
            },
            month("2026-10"),
        );
        assert_eq!(
            quiet,
            "CSPaper reviews from this machine in 2026-10: 3 completed, 1 in progress"
        );
        let busy = usage_note(
            ProviderUsage {
                completed: 1,
                in_progress: 0,
                ended: 2,
                uncertain: 1,
            },
            month("2026-10"),
        );
        assert_eq!(
            busy,
            "CSPaper reviews from this machine in 2026-10: 1 completed, 0 in progress, 2 ended without a review; 1 uncertain submission(s) may also count"
        );
    }

    #[test]
    fn billing_months_parse_and_bound_in_utc() {
        let october = month("2026-10");
        assert_eq!(october.to_string(), "2026-10");
        assert_eq!(
            october.range(),
            (at("2026-10-01T00:00:00Z"), at("2026-11-01T00:00:00Z"))
        );
        assert!(october.contains(at("2026-10-31T23:59:59Z")));
        assert!(!october.contains(at("2026-11-01T00:00:00Z")));
        // Early on 1 November in UTC+2 is still October in UTC.
        assert!(october.contains(at("2026-11-01T01:30:00+02:00")));
        assert_eq!(month("2026-12").range().1, at("2027-01-01T00:00:00Z"));
        assert_eq!(
            BillingMonth::containing(at("2026-02-14T08:00:00Z")),
            month("2026-02")
        );
        for bad in ["2026-13", "2026-0", "26-10", "2026/10", "", "2026-10-01"] {
            assert!(BillingMonth::parse(bad).is_none(), "{bad:?}");
        }
    }

    fn remote(id: &str, status: &str, created: Option<&str>) -> RemoteJob {
        RemoteJob {
            id: id.to_string(),
            status: status.to_string(),
            created: created.map(at),
        }
    }

    fn listed(jobs: Vec<RemoteJob>, incomplete: bool) -> RemoteJobList {
        RemoteJobList { jobs, incomplete }
    }

    #[test]
    fn org_usage_counts_the_month_and_all_months_by_status_and_client() {
        let oct = Some("2026-10-05T10:00:00Z");
        let sep = Some("2026-09-30T23:00:00Z");
        let jobs = vec![
            remote("a", "COMPLETED", oct),
            remote("b", "COMPLETED", oct),
            remote("c", "PROCESSING", oct),
            remote("d", "PENDING", oct),
            remote("e", "FAILED", oct),
            remote("f", "ARCHIVED", oct),
            remote("g", "COMPLETED", sep),
            remote("h", "COMPLETED", None),
        ];
        let local: HashSet<String> = ["a", "c", "g", "zz"].map(str::to_string).into();
        let report = summarize(&listed(jobs, false), &local, month("2026-10"));
        assert_eq!(
            report.in_month,
            OrgUsage {
                total: 6,
                completed: 2,
                in_progress: 2,
                failed: 1,
                other: 1,
                via_reviewloop: 2,
            }
        );
        assert_eq!(report.in_month.via_other_clients(), 4);
        assert_eq!(report.all_time.total, 8);
        assert_eq!(report.all_time.via_reviewloop, 3);
        assert_eq!(report.undated, 1);
        assert!(!report.list_incomplete);
    }

    #[test]
    fn org_usage_report_shows_what_is_left_of_the_monthly_allowance() {
        let jobs = (0..12)
            .map(|n| {
                let status = match n {
                    0..=8 => "COMPLETED",
                    9 => "PROCESSING",
                    _ => "FAILED",
                };
                remote(&format!("j{n}"), status, Some("2026-10-05T10:00:00Z"))
            })
            .collect();
        let local: HashSet<String> = (0..7).map(|n| format!("j{n}")).collect();
        let report = summarize(&listed(jobs, false), &local, month("2026-10"));
        assert_eq!(
            org_usage_report(&report, Some(50)),
            "CSPaper organisation reviews in 2026-10 (UTC), all clients: 12 (9 completed, 1 in progress, 2 failed)\n  via reviewloop on this machine (submitted or imported): 7\n  via other clients: 5\n  monthly allowance 50, 38 left\nAll months: 12 reviews, 7 via reviewloop on this machine"
        );
        assert_eq!(allowance_left(&report, Some(50)), Some(38));
        assert!(org_usage_report(&report, Some(10)).contains("monthly allowance 10, 2 over"),);
        assert_eq!(allowance_left(&report, Some(10)), Some(-2));
        assert_eq!(allowance_left(&report, None), None);
        let unset = org_usage_report(&report, None);
        assert!(unset.contains("no monthly allowance set"), "{unset}");
        assert!(
            unset.contains("providers.cspaper.monthly_allowance"),
            "{unset}"
        );
        assert!(!unset.to_lowercase().contains("credit"), "{unset}");

        let partial = summarize(
            &listed(vec![remote("x", "COMPLETED", None)], true),
            &local,
            month("2026-10"),
        );
        let text = org_usage_report(&partial, None);
        assert!(text.contains("1 job(s) without a creation date"), "{text}");
        assert!(text.ends_with("the counts may be low)"), "{text}");
    }

    #[test]
    fn job_list_entries_need_an_id_and_a_status() {
        let listed = parse_job_list(&json!({"status": 200, "data": [
            {"id": "a", "status": "completed", "agent_id": "ICLR_main_2026_1", "created": "2025-01-01T00:00:00Z"},
            {"id": "b", "status": "PENDING", "created": "not a date"},
        ]}))
        .unwrap();
        assert_eq!(
            listed,
            vec![
                remote("a", "COMPLETED", Some("2025-01-01T00:00:00Z")),
                remote("b", "PENDING", None),
            ]
        );
        for payload in [
            json!({"status": 200}),
            json!({"status": 200, "data": {"id": "a"}}),
            json!({"status": 200, "data": [{"id": "a"}]}),
            json!({"status": 200, "data": [{"status": "COMPLETED"}]}),
        ] {
            assert!(
                matches!(parse_job_list(&payload), Err(BackendError::Schema(_))),
                "{payload}"
            );
        }
    }

    #[test]
    fn job_ids_are_path_safe() {
        assert!(is_job_id(JOB));
        assert!(is_job_id("abc_DEF-123"));
        for bad in ["", "a/b", "a?b", "a b", "..", &"x".repeat(129)] {
            assert!(!is_job_id(bad), "{bad:?}");
        }
    }

    #[test]
    fn desk_rejection_option_is_sent_verbatim_or_defaulted() {
        let options = |value: &str| ReviewOptions::default().with(DESK_REJECTION_ENABLED, value);
        assert_eq!(
            desk_rejection_field(&ReviewOptions::default()).unwrap(),
            "true"
        );
        assert_eq!(desk_rejection_field(&options("false")).unwrap(), "false");
        assert!(matches!(
            desk_rejection_field(&options("no")),
            Err(BackendError::Rejected(_))
        ));
    }

    #[test]
    fn quoted_errors_prefer_the_envelope_message_and_scrub_the_key() {
        let backend = CspaperBackend {
            client: reqwest_middleware::ClientBuilder::new(reqwest::Client::new()).build(),
            base_url: "https://cspaper.org".into(),
            api_key: Some(Redacted("csp_live_SECRET".into())),
        };
        let forbidden = fixture("error_403.json").to_string();
        assert_eq!(backend.quote(&forbidden), "Invalid API Key");
        assert_eq!(
            backend.quote("upstream said csp_live_SECRET is bad"),
            "upstream said [redacted] is bad"
        );
        let long = "x".repeat(MAX_QUOTED_CHARS + 10);
        assert_eq!(backend.quote(&long).chars().count(), MAX_QUOTED_CHARS + 1);
        assert_eq!(backend.quote("  "), "<empty body>");
    }
}
