use crate::{
    config::Config,
    db::Db,
    model::{Job, JobStatus},
    token::{extract_review_token, extract_token_with_pattern},
};
use anyhow::Result;
use chrono::Utc;
use regex::Regex;
use std::collections::{BTreeMap, HashSet};
use tracing::Instrument;

#[derive(Debug, Clone)]
struct EmailMatch {
    backend: String,
    token: String,
}

fn detect_backend_from_header(
    header_text: &str,
    patterns: &BTreeMap<String, String>,
) -> Option<String> {
    for (backend, pattern) in patterns {
        if let Ok(re) = Regex::new(pattern)
            && re.is_match(header_text)
        {
            return Some(backend.clone());
        }
    }
    None
}

fn extract_match(
    text: &str,
    backend_patterns: &BTreeMap<String, String>,
    backend_hint: Option<&str>,
) -> Option<EmailMatch> {
    if let Some(backend) = backend_hint {
        if let Some(pattern) = backend_patterns.get(backend)
            && let Some(token) = extract_token_with_pattern(text, pattern)
        {
            return Some(EmailMatch {
                backend: backend.to_string(),
                token,
            });
        }
        if let Some(token) = extract_review_token(text) {
            return Some(EmailMatch {
                backend: backend.to_string(),
                token,
            });
        }
    }

    for (backend, pattern) in backend_patterns {
        if let Some(token) = extract_token_with_pattern(text, pattern) {
            return Some(EmailMatch {
                backend: backend.clone(),
                token,
            });
        }
    }

    extract_review_token(text).map(|token| EmailMatch {
        backend: "stanford".to_string(),
        token,
    })
}

fn bind_matches(
    db: &Db,
    project_id: &str,
    source: &str,
    matches: Vec<EmailMatch>,
) -> Result<Vec<Job>> {
    let mut seen_tokens = HashSet::new();
    let mut affected: Vec<Job> = Vec::new();

    for matched in matches {
        if !seen_tokens.insert(matched.token.clone()) {
            continue;
        }

        db.record_email_token(
            &matched.token,
            &format!("{source}:{}", matched.backend),
            Some(&format!("{source}_unseen")),
        )?;

        if let Some(existing_job) = db.find_job_by_token(project_id, &matched.token)? {
            if should_nudge_poll_now(existing_job.status) {
                db.update_job_state(
                    &existing_job.id,
                    JobStatus::Processing,
                    Some(existing_job.attempt),
                    Some(Some(Utc::now())),
                    None,
                )?;
                db.add_event(
                    None,
                    Some(&existing_job.id),
                    &format!("{source}_token_nudged_poll"),
                    serde_json::json!({
                        "token": matched.token,
                        "backend": matched.backend,
                        "source": source
                    }),
                )?;
                // Return the fresh job state so the caller can immediately poll it.
                if let Some(fresh) = db.get_job(&existing_job.id)? {
                    affected.push(fresh);
                }
                continue;
            }
            db.add_event(
                None,
                Some(&existing_job.id),
                &format!("{source}_token_already_bound"),
                serde_json::json!({
                    "token": matched.token,
                    "backend": matched.backend,
                    "source": source
                }),
            )?;
            continue;
        }

        if let Some(job) = db.find_latest_open_job_without_token(project_id, &matched.backend)? {
            let next_poll = Utc::now();
            db.attach_token_to_job(&job.id, &matched.token, next_poll)?;
            db.add_event(
                None,
                Some(&job.id),
                &format!("{source}_token_attached"),
                serde_json::json!({
                    "token": matched.token,
                    "backend": matched.backend,
                    "source": source
                }),
            )?;
            // Return the fresh job state (now with token set) so the caller can immediately poll.
            if let Some(fresh) = db.get_job(&job.id)? {
                affected.push(fresh);
            }
        }
    }

    Ok(affected)
}

fn should_nudge_poll_now(status: JobStatus) -> bool {
    !matches!(
        status,
        JobStatus::Completed
            | JobStatus::Failed
            | JobStatus::FailedNeedsManual
            | JobStatus::Timeout
    )
}

#[cfg(feature = "imap")]
mod imap_impl {
    use crate::{
        config::{Config, ImapConfig},
        db::Db,
        email::{EmailMatch, bind_matches, detect_backend_from_header, extract_match},
    };
    use anyhow::{Context, Result};
    use chrono::{Duration, Utc};
    use native_tls::TlsConnector;

    pub async fn poll_imap_if_enabled(config: &Config, db: &Db) -> Result<Vec<crate::model::Job>> {
        let Some(imap_cfg) = &config.imap else {
            return Ok(vec![]);
        };
        if !imap_cfg.enabled {
            return Ok(vec![]);
        }
        if imap_cfg.username.trim().is_empty() || imap_cfg.password.trim().is_empty() {
            return Ok(vec![]);
        }

        let imap_cfg = imap_cfg.clone();
        let matches = tokio::task::spawn_blocking(move || poll_once_blocking(&imap_cfg))
            .await
            .context("IMAP polling task failed to join")??;

        bind_matches(db, &config.project_id, "imap", matches)
    }

    fn poll_once_blocking(imap_cfg: &ImapConfig) -> Result<Vec<EmailMatch>> {
        let tls = TlsConnector::builder().build()?;
        let client = imap::connect(
            (imap_cfg.server.as_str(), imap_cfg.port),
            imap_cfg.server.as_str(),
            &tls,
        )
        .with_context(|| {
            format!(
                "failed to connect IMAP server {}:{}",
                imap_cfg.server, imap_cfg.port
            )
        })?;

        let mut session = client
            .login(&imap_cfg.username, &*imap_cfg.password)
            .map_err(|e| e.0)
            .context("IMAP login failed")?;

        session
            .select(&imap_cfg.folder)
            .with_context(|| format!("failed to select IMAP folder: {}", imap_cfg.folder))?;

        let search_query = build_unseen_search_query(imap_cfg, Utc::now());
        let unseen = session.search(search_query)?;
        let mut unseen_ids: Vec<u32> = unseen.into_iter().collect();
        unseen_ids.sort_unstable_by(|a, b| b.cmp(a));
        unseen_ids.truncate(imap_cfg.max_messages_per_poll);
        let mut matches = Vec::new();

        for id in unseen_ids {
            let backend_hint = if imap_cfg.header_first {
                fetch_header_text(&mut session, id).and_then(|headers| {
                    detect_backend_from_header(&headers, &imap_cfg.backend_header_patterns)
                })
            } else {
                None
            };

            if imap_cfg.header_first && backend_hint.is_none() {
                continue;
            }

            let fetches = session.fetch(id.to_string(), "RFC822")?;
            let mut matched_for_message: Option<EmailMatch> = None;
            for fetch in &fetches {
                if let Some(body) = fetch.body() {
                    let text = String::from_utf8_lossy(body);
                    if let Some(matched) =
                        extract_match(&text, &imap_cfg.backend_patterns, backend_hint.as_deref())
                    {
                        matched_for_message = Some(matched);
                        break;
                    }
                }
            }

            if let Some(matched) = matched_for_message {
                matches.push(matched);
                if imap_cfg.mark_seen {
                    let _ = session.store(id.to_string(), "+FLAGS (\\Seen)");
                }
            }
        }

        let _ = session.logout();
        Ok(matches)
    }

    pub(super) fn build_unseen_search_query(
        imap_cfg: &ImapConfig,
        now: chrono::DateTime<Utc>,
    ) -> String {
        if imap_cfg.max_lookback_hours == 0 {
            return "UNSEEN".to_string();
        }
        let since = (now - Duration::hours(imap_cfg.max_lookback_hours as i64))
            .format("%d-%b-%Y")
            .to_string();
        format!("UNSEEN SINCE {since}")
    }

    fn fetch_header_text(
        session: &mut imap::Session<native_tls::TlsStream<std::net::TcpStream>>,
        id: u32,
    ) -> Option<String> {
        let fetches = session
            .fetch(id.to_string(), "BODY.PEEK[HEADER.FIELDS (SUBJECT FROM)]")
            .ok()?;
        for fetch in &fetches {
            if let Some(body) = fetch.body() {
                return Some(String::from_utf8_lossy(body).to_string());
            }
        }
        None
    }
}

mod gmail_impl {
    use crate::{
        config::{Config, GmailOauthConfig},
        db::Db,
        email::{EmailMatch, bind_matches, detect_backend_from_header, extract_match},
        oauth::{self, OauthProvider, google::GoogleOauthProvider},
    };
    use anyhow::{Context, Result};
    use base64::{
        Engine as _,
        engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD},
    };
    use serde::Deserialize;
    use serde_json::json;

    #[derive(Debug, Deserialize)]
    struct GmailListResponse {
        messages: Option<Vec<GmailMessageRef>>,
    }

    #[derive(Debug, Deserialize)]
    struct GmailMessageRef {
        id: String,
    }

    #[derive(Debug, Deserialize)]
    struct GmailMessageResponse {
        snippet: Option<String>,
        payload: Option<GmailPayload>,
    }

    #[derive(Debug, Deserialize)]
    struct GmailPayload {
        headers: Option<Vec<GmailHeader>>,
        body: Option<GmailBody>,
        parts: Option<Vec<GmailPayload>>,
    }

    #[derive(Debug, Deserialize)]
    struct GmailHeader {
        name: String,
        value: String,
    }

    #[derive(Debug, Deserialize)]
    struct GmailBody {
        data: Option<String>,
    }

    const GMAIL_TOKEN_REFRESH_SKEW_SECONDS: i64 = 5 * 60;
    const GMAIL_REAUTH_HINT: &str = "run `reviewloop email login` to re-authenticate.";

    #[derive(Debug, thiserror::Error)]
    #[error("gmail oauth token refresh failed: {source}")]
    struct GmailOauthRefreshError {
        #[source]
        source: anyhow::Error,
    }

    pub async fn poll_gmail_if_enabled(config: &Config, db: &Db) -> Result<Vec<crate::model::Job>> {
        let Some(gmail_cfg) = &config.gmail_oauth else {
            return Ok(vec![]);
        };
        if !gmail_cfg.enabled {
            return Ok(vec![]);
        }

        let Some(provider) = GoogleOauthProvider::from_config(config)? else {
            return Ok(vec![]);
        };
        poll_gmail_with_provider(config, db, gmail_cfg, &provider).await
    }

    pub(super) async fn poll_gmail_with_provider(
        config: &Config,
        db: &Db,
        gmail_cfg: &GmailOauthConfig,
        provider: &dyn OauthProvider,
    ) -> Result<Vec<crate::model::Job>> {
        let client = crate::http::build_reqwest_client(config)?;
        let matches = match fetch_matches(gmail_cfg, provider, &client).await {
            Ok(matches) => matches,
            Err(err) if err.downcast_ref::<GmailOauthRefreshError>().is_some() => {
                tracing::warn!(
                    error = %err,
                    hint = GMAIL_REAUTH_HINT,
                    "Gmail token refresh failed; run `reviewloop email login` to re-authenticate."
                );
                // Surface the failure as a db event so `daemon status` can
                // highlight it without requiring the user to check logs.
                let _ = db.add_event(
                    Some(&config.project_id),
                    None,
                    "gmail_oauth_refresh_failed",
                    json!({"error": err.to_string(), "hint": GMAIL_REAUTH_HINT}),
                );
                return Ok(vec![]);
            }
            Err(err) => return Err(err),
        };

        bind_matches(db, &config.project_id, "gmail", matches)
    }

    pub(super) async fn load_daemon_gmail_access_token(
        provider: &dyn OauthProvider,
    ) -> Result<String> {
        oauth::ensure_valid_access_token_with_skew(provider, GMAIL_TOKEN_REFRESH_SKEW_SECONDS)
            .await
            .map_err(|source| GmailOauthRefreshError { source }.into())
    }

    async fn fetch_matches(
        cfg: &GmailOauthConfig,
        provider: &dyn OauthProvider,
        client: &reqwest::Client,
    ) -> Result<Vec<EmailMatch>> {
        let query = build_unread_query(cfg);
        let max_results = cfg.max_messages_per_poll.to_string();

        let access_token = load_daemon_gmail_access_token(provider).await?;
        let list_resp = client
            .get("https://gmail.googleapis.com/gmail/v1/users/me/messages")
            .bearer_auth(&access_token)
            .query(&[("q", query.as_str()), ("maxResults", max_results.as_str())])
            .send()
            .await
            .context("gmail list messages request failed")?;

        if !list_resp.status().is_success() {
            let body = list_resp.text().await.unwrap_or_else(|_| "".to_string());
            return Err(anyhow::anyhow!("gmail list messages failed: {body}"));
        }

        let list_payload: GmailListResponse = list_resp
            .json()
            .await
            .context("invalid gmail list messages payload")?;

        let mut matches = Vec::new();

        for msg in list_payload.messages.unwrap_or_default() {
            let access_token = load_daemon_gmail_access_token(provider).await?;
            let metadata = fetch_message_metadata(client, &access_token, &msg.id).await?;
            let header_text = metadata
                .payload
                .as_ref()
                .and_then(|p| p.headers.as_ref())
                .map(render_header_text)
                .unwrap_or_default();

            let backend_hint = if cfg.header_first {
                detect_backend_from_header(&header_text, &cfg.backend_header_patterns)
            } else {
                None
            };

            if cfg.header_first && backend_hint.is_none() {
                continue;
            }

            let access_token = load_daemon_gmail_access_token(provider).await?;
            let full = fetch_message_full(client, &access_token, &msg.id).await?;
            let mut text = String::new();
            if let Some(snippet) = full.snippet {
                text.push_str(&snippet);
                text.push('\n');
            }
            if let Some(payload) = &full.payload {
                append_payload_text(payload, &mut text);
            }

            if let Some(matched) =
                extract_match(&text, &cfg.backend_patterns, backend_hint.as_deref())
            {
                matches.push(matched);
                if cfg.mark_seen {
                    let access_token = load_daemon_gmail_access_token(provider).await?;
                    mark_message_seen(client, &access_token, &msg.id).await?;
                }
            }
        }

        Ok(matches)
    }

    async fn fetch_message_metadata(
        client: &reqwest::Client,
        access_token: &str,
        message_id: &str,
    ) -> Result<GmailMessageResponse> {
        let resp = client
            .get(format!(
                "https://gmail.googleapis.com/gmail/v1/users/me/messages/{message_id}"
            ))
            .bearer_auth(access_token)
            .query(&[
                ("format", "metadata"),
                ("metadataHeaders", "From"),
                ("metadataHeaders", "Subject"),
            ])
            .send()
            .await
            .context("gmail metadata request failed")?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_else(|_| "".to_string());
            return Err(anyhow::anyhow!("gmail metadata failed: {body}"));
        }

        resp.json().await.context("invalid gmail metadata payload")
    }

    async fn fetch_message_full(
        client: &reqwest::Client,
        access_token: &str,
        message_id: &str,
    ) -> Result<GmailMessageResponse> {
        let resp = client
            .get(format!(
                "https://gmail.googleapis.com/gmail/v1/users/me/messages/{message_id}"
            ))
            .bearer_auth(access_token)
            .query(&[("format", "full")])
            .send()
            .await
            .context("gmail full message request failed")?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_else(|_| "".to_string());
            return Err(anyhow::anyhow!("gmail full message failed: {body}"));
        }

        resp.json()
            .await
            .context("invalid gmail full message payload")
    }

    async fn mark_message_seen(
        client: &reqwest::Client,
        access_token: &str,
        message_id: &str,
    ) -> Result<()> {
        let resp = client
            .post(format!(
                "https://gmail.googleapis.com/gmail/v1/users/me/messages/{message_id}/modify"
            ))
            .bearer_auth(access_token)
            .json(&serde_json::json!({
                "removeLabelIds": ["UNREAD"]
            }))
            .send()
            .await
            .context("gmail modify message request failed")?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_else(|_| "".to_string());
            return Err(anyhow::anyhow!("gmail modify message failed: {body}"));
        }

        Ok(())
    }

    fn render_header_text(headers: &Vec<GmailHeader>) -> String {
        let mut out = String::new();
        for h in headers {
            out.push_str(&h.name);
            out.push_str(": ");
            out.push_str(&h.value);
            out.push('\n');
        }
        out
    }

    fn append_payload_text(payload: &GmailPayload, out: &mut String) {
        if let Some(body) = &payload.body
            && let Some(data) = &body.data
            && let Some(decoded) = decode_base64url_to_text(data)
        {
            out.push_str(&decoded);
            out.push('\n');
        }
        if let Some(parts) = &payload.parts {
            for part in parts {
                append_payload_text(part, out);
            }
        }
    }

    fn decode_base64url_to_text(data: &str) -> Option<String> {
        let decoded = URL_SAFE_NO_PAD
            .decode(data)
            .or_else(|_| URL_SAFE.decode(data))
            .ok()?;
        Some(String::from_utf8_lossy(&decoded).to_string())
    }

    pub(super) fn build_unread_query(cfg: &GmailOauthConfig) -> String {
        if cfg.max_lookback_hours == 0 {
            return "is:unread".to_string();
        }
        let days = cfg.max_lookback_hours.div_ceil(24).max(1);
        format!("is:unread newer_than:{days}d")
    }
}

pub async fn poll_imap_if_enabled(config: &Config, db: &Db) -> Result<Vec<Job>> {
    let span = tracing::info_span!("poll_imap_if_enabled", project_id = %config.project_id);
    async move {
        #[allow(unused_mut)]
        let mut affected = gmail_impl::poll_gmail_if_enabled(config, db).await?;
        #[cfg(feature = "imap")]
        affected.extend(imap_impl::poll_imap_if_enabled(config, db).await?);
        #[cfg(not(feature = "imap"))]
        {
            use std::sync::OnceLock;
            static WARNED: OnceLock<()> = OnceLock::new();
            if config.imap.as_ref().map(|c| c.enabled).unwrap_or(false) {
                WARNED.get_or_init(|| {
                    tracing::warn!(
                        "imap.enabled = true but this binary was built without --features imap; \
                         install with 'cargo install reviewloop --features imap' to enable IMAP token ingestion"
                    );
                });
            }
        }
        Ok(affected)
    }
    .instrument(span)
    .await
}

#[cfg(test)]
mod tests {
    use crate::{
        config::{Config, GmailOauthConfig, ImapConfig},
        db::Db,
        email::detect_backend_from_header,
        model::{JobStatus, NewJob},
        oauth::{
            DeviceCodePoll, DeviceCodeStart, OauthProvider, OauthTokenRecord, OauthTokenResponse,
            load_token_record, save_token_record,
        },
    };
    use async_trait::async_trait;
    #[cfg(feature = "imap")]
    use chrono::TimeZone;
    use chrono::{DateTime, Duration, Utc};
    use std::{
        path::PathBuf,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    struct MockOauthProvider {
        path: PathBuf,
        refresh_calls: Arc<AtomicUsize>,
        refresh_results: Arc<Mutex<Vec<std::result::Result<OauthTokenResponse, String>>>>,
    }

    impl MockOauthProvider {
        fn new(
            path: PathBuf,
            refresh_results: Vec<std::result::Result<OauthTokenResponse, String>>,
        ) -> Self {
            Self {
                path,
                refresh_calls: Arc::new(AtomicUsize::new(0)),
                refresh_results: Arc::new(Mutex::new(refresh_results)),
            }
        }

        fn refresh_calls(&self) -> usize {
            self.refresh_calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl OauthProvider for MockOauthProvider {
        fn name(&self) -> &'static str {
            "mock-google"
        }

        fn token_store_path(&self) -> PathBuf {
            self.path.clone()
        }

        async fn start_device_flow(&self) -> anyhow::Result<DeviceCodeStart> {
            unimplemented!()
        }

        async fn poll_device_flow(&self, _: &str) -> anyhow::Result<DeviceCodePoll> {
            unimplemented!()
        }

        async fn refresh_access_token(
            &self,
            refresh_token: &str,
        ) -> anyhow::Result<OauthTokenResponse> {
            if refresh_token != "refresh-token" {
                anyhow::bail!("unexpected refresh token: {refresh_token}");
            }
            self.refresh_calls.fetch_add(1, Ordering::SeqCst);
            let mut results = self.refresh_results.lock().expect("refresh results mutex");
            match results.remove(0) {
                Ok(response) => Ok(response),
                Err(message) => Err(anyhow::anyhow!(message)),
            }
        }
    }

    fn unique_oauth_token_path(test_name: &str) -> PathBuf {
        let dir = std::env::current_dir()
            .expect("cwd")
            .join("target")
            .join("email-oauth-tests")
            .join(format!("{}-{}", test_name, uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("create test oauth dir");
        dir.join("google_token.json")
    }

    fn token_record(access_token: &str, expires_at: DateTime<Utc>) -> OauthTokenRecord {
        OauthTokenRecord {
            refresh_token: "refresh-token".to_string(),
            access_token: access_token.to_string(),
            expires_at,
            expires_at_unix: expires_at.timestamp(),
            scope: Some("gmail.readonly".to_string()),
            token_type: Some("bearer".to_string()),
            updated_at_unix: Utc::now().timestamp(),
        }
    }

    #[test]
    fn header_match_detects_stanford_sender() {
        let cfg = ImapConfig::default();
        let header =
            "From: Stanford Agentic Reviewer <review@mail.paperreview.ai>\r\nSubject: hello\r\n";
        let backend = detect_backend_from_header(header, &cfg.backend_header_patterns);
        assert_eq!(backend.as_deref(), Some("stanford"));
    }

    #[test]
    fn header_match_returns_none_for_unrelated_mail() {
        let cfg = ImapConfig::default();
        let header = "From: notifications@github.com\r\nSubject: PR updated\r\n";
        let backend = detect_backend_from_header(header, &cfg.backend_header_patterns);
        assert!(backend.is_none());
    }

    #[cfg(feature = "imap")]
    #[test]
    fn imap_search_query_applies_lookback_window() {
        let cfg = ImapConfig {
            max_lookback_hours: 72,
            ..ImapConfig::default()
        };
        let now = Utc.with_ymd_and_hms(2026, 3, 5, 12, 0, 0).unwrap();
        let query = super::imap_impl::build_unseen_search_query(&cfg, now);
        assert_eq!(query, "UNSEEN SINCE 02-Mar-2026");
    }

    #[cfg(feature = "imap")]
    #[test]
    fn imap_search_query_can_disable_lookback() {
        let cfg = ImapConfig {
            max_lookback_hours: 0,
            ..ImapConfig::default()
        };
        let now = Utc.with_ymd_and_hms(2026, 3, 5, 12, 0, 0).unwrap();
        let query = super::imap_impl::build_unseen_search_query(&cfg, now);
        assert_eq!(query, "UNSEEN");
    }

    #[test]
    fn gmail_query_respects_lookback_window() {
        let cfg = GmailOauthConfig {
            max_lookback_hours: 72,
            ..GmailOauthConfig::default()
        };
        let query = super::gmail_impl::build_unread_query(&cfg);
        assert_eq!(query, "is:unread newer_than:3d");
    }

    #[test]
    fn gmail_query_can_disable_lookback_window() {
        let cfg = GmailOauthConfig {
            max_lookback_hours: 0,
            ..GmailOauthConfig::default()
        };
        let query = super::gmail_impl::build_unread_query(&cfg);
        assert_eq!(query, "is:unread");
    }

    #[tokio::test]
    async fn preemptive_refresh_called_when_token_about_to_expire() {
        let token_path = unique_oauth_token_path("preemptive-refresh");
        let provider = MockOauthProvider::new(
            token_path.clone(),
            vec![Ok(OauthTokenResponse {
                access_token: "fresh-access-token".to_string(),
                refresh_token: None,
                expires_in_seconds: 3600,
                scope: Some("gmail.readonly".to_string()),
                token_type: Some("bearer".to_string()),
            })],
        );
        let expiring_at = Utc::now() + Duration::minutes(2);
        save_token_record(&provider, &token_record("stale-access-token", expiring_at))
            .expect("save expiring token");

        let before_refresh = Utc::now();
        let access_token = super::gmail_impl::load_daemon_gmail_access_token(&provider)
            .await
            .expect("refresh succeeds");

        assert_eq!(access_token, "fresh-access-token");
        assert_eq!(provider.refresh_calls(), 1);
        let saved = load_token_record(&provider)
            .expect("load saved token")
            .expect("token exists");
        assert_eq!(saved.access_token, "fresh-access-token");
        assert_eq!(saved.refresh_token, "refresh-token");
        assert!(saved.expires_at > before_refresh + Duration::minutes(55));
        assert_eq!(saved.expires_at_unix, saved.expires_at.timestamp());
        let raw = std::fs::read_to_string(token_path).expect("read saved token json");
        assert!(raw.contains("\"expires_at\""));
    }

    #[tokio::test]
    async fn refresh_failure_does_not_crash_poll_loop() {
        let token_path = unique_oauth_token_path("refresh-failure");
        let provider = MockOauthProvider::new(
            token_path.clone(),
            vec![Err("refresh endpoint returned 401".to_string())],
        );
        let expiring_at = Utc::now() + Duration::minutes(2);
        save_token_record(&provider, &token_record("stale-access-token", expiring_at))
            .expect("save expiring token");

        let mut config = Config {
            project_id: "proj-refresh-failure".to_string(),
            ..Config::default()
        };
        let state_dir = token_path.parent().expect("token parent").join("state");
        std::fs::create_dir_all(&state_dir).expect("create state dir");
        config.core.state_dir = state_dir.to_string_lossy().to_string();
        let gmail_cfg = GmailOauthConfig {
            enabled: true,
            client_id: "dummy-client-id".to_string(),
            token_store_path: Some(token_path.to_string_lossy().to_string()),
            ..GmailOauthConfig::default()
        };
        config.gmail_oauth = Some(gmail_cfg.clone());
        let db = Db::new_in_memory("email_refresh_failure_no_crash").expect("in-memory db");
        db.init_schema().expect("init schema");

        let result =
            super::gmail_impl::poll_gmail_with_provider(&config, &db, &gmail_cfg, &provider).await;

        assert!(result.is_ok(), "poll must not crash: {result:?}");
        assert!(result.expect("poll ok").is_empty());
        assert_eq!(provider.refresh_calls(), 1);
        let ev = db
            .most_recent_event_of_type(&config.project_id, "gmail_oauth_refresh_failed")
            .expect("db query")
            .expect("expected gmail_oauth_refresh_failed event");
        assert!(
            ev.payload
                .get("error")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|error| error.contains("refresh endpoint returned 401")),
            "event payload must contain the refresh failure"
        );
        assert_eq!(
            ev.payload.get("hint").and_then(serde_json::Value::as_str),
            Some("run `reviewloop email login` to re-authenticate.")
        );
    }

    #[test]
    fn existing_processing_token_nudges_poll_to_now() {
        let db = Db::new_in_memory("email_nudge_poll_now").expect("in-memory db");
        db.init_schema().expect("init schema");

        let seed = NewJob {
            project_id: "project-email".to_string(),
            paper_id: "paper-a".to_string(),
            backend: "stanford".to_string(),
            pdf_path: "paper.pdf".to_string(),
            pdf_hash: "hash-a".to_string(),
            status: JobStatus::Queued,
            email: "user@example.com".to_string(),
            venue: None,
            git_tag: None,
            git_commit: None,
            next_poll_at: None,
        };
        let job = db.create_job(&seed).expect("create job");
        let far_future = Utc::now() + Duration::hours(1);
        db.mark_submitted_with_token(&job.id, "tok_existing", far_future)
            .expect("mark submitted");

        super::bind_matches(
            &db,
            "project-email",
            "gmail",
            vec![super::EmailMatch {
                backend: "stanford".to_string(),
                token: "tok_existing".to_string(),
            }],
        )
        .expect("bind matches");

        let updated = db.get_job(&job.id).expect("get job").expect("job exists");
        assert_eq!(updated.status, JobStatus::Processing);
        let next_poll_at = updated.next_poll_at.expect("next poll exists");
        assert!(next_poll_at < far_future);
    }

    #[test]
    fn terminal_job_token_does_not_nudge_poll() {
        let db = Db::new_in_memory("email_terminal_no_nudge").expect("in-memory db");
        db.init_schema().expect("init schema");

        let seed = NewJob {
            project_id: "project-email".to_string(),
            paper_id: "paper-b".to_string(),
            backend: "stanford".to_string(),
            pdf_path: "paper.pdf".to_string(),
            pdf_hash: "hash-b".to_string(),
            status: JobStatus::Queued,
            email: "user@example.com".to_string(),
            venue: None,
            git_tag: None,
            git_commit: None,
            next_poll_at: None,
        };
        let job = db.create_job(&seed).expect("create job");
        let far_future = Utc::now() + Duration::hours(2);
        db.mark_submitted_with_token(&job.id, "tok_terminal", far_future)
            .expect("mark submitted");
        db.update_job_state(
            &job.id,
            JobStatus::Completed,
            Some(3),
            Some(Some(far_future)),
            None,
        )
        .expect("mark completed");

        super::bind_matches(
            &db,
            "project-email",
            "gmail",
            vec![super::EmailMatch {
                backend: "stanford".to_string(),
                token: "tok_terminal".to_string(),
            }],
        )
        .expect("bind matches");

        let updated = db.get_job(&job.id).expect("get job").expect("job exists");
        assert_eq!(updated.status, JobStatus::Completed);
        assert_eq!(updated.attempt, 3);
        assert_eq!(updated.next_poll_at, Some(far_future));
    }

    /// U6 regression: when the Gmail OAuth token is absent (simulates a
    /// revoked / missing refresh token), `poll_gmail_if_enabled` must write a
    /// `gmail_oauth_refresh_failed` event so `daemon status` can surface it.
    #[tokio::test]
    async fn gmail_poll_writes_oauth_refresh_failed_event_on_error() {
        let token_path = unique_oauth_token_path("missing-token");
        let state_dir = token_path.parent().expect("token parent").join("state");
        std::fs::create_dir_all(&state_dir).expect("create state_dir");

        // Point to a token file that does not exist — ensure_valid_access_token
        // returns Err("no oauth token found …") which is the same error path
        // as a revoked refresh token.
        std::fs::remove_file(&token_path).ok();

        let mut config = Config {
            project_id: "proj-u6-gmail".to_string(),
            ..Config::default()
        };
        config.core.state_dir = state_dir.to_string_lossy().to_string();
        config.gmail_oauth = Some(GmailOauthConfig {
            enabled: true,
            client_id: "dummy-client-id-u6".to_string(),
            token_store_path: Some(token_path.to_string_lossy().to_string()),
            ..GmailOauthConfig::default()
        });

        let db = Db::new_in_memory("email_u6_oauth_refresh").expect("in-memory db");
        db.init_schema().expect("init schema");

        // Should succeed (returns Ok with empty vec) and write the event.
        let result = super::gmail_impl::poll_gmail_if_enabled(&config, &db).await;
        assert!(
            result.is_ok(),
            "poll must not propagate the error: {result:?}"
        );
        assert!(result.unwrap().is_empty());

        let ev = db
            .most_recent_event_of_type(&config.project_id, "gmail_oauth_refresh_failed")
            .expect("db query")
            .expect("expected gmail_oauth_refresh_failed event");
        assert!(
            ev.payload.get("error").is_some(),
            "event payload must contain an 'error' field"
        );
    }
}
