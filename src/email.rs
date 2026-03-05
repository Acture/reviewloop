use crate::{
    config::Config,
    db::Db,
    token::{extract_review_token, extract_token_with_pattern},
};
use anyhow::Result;
use chrono::Utc;
use regex::Regex;
use std::collections::{BTreeMap, HashSet};

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

fn bind_matches(db: &Db, source: &str, matches: Vec<EmailMatch>) -> Result<()> {
    let mut seen_tokens = HashSet::new();

    for matched in matches {
        if !seen_tokens.insert(matched.token.clone()) {
            continue;
        }

        db.record_email_token(
            &matched.token,
            &format!("{source}:{}", matched.backend),
            Some(&format!("{source}_unseen")),
        )?;

        if let Some(existing_job) = db.find_job_by_token(&matched.token)? {
            db.add_event(
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

        if let Some(job) = db.find_latest_open_job_without_token(&matched.backend)? {
            let next_poll = Utc::now();
            db.attach_token_to_job(&job.id, &matched.token, next_poll)?;
            db.add_event(
                Some(&job.id),
                &format!("{source}_token_attached"),
                serde_json::json!({
                    "token": matched.token,
                    "backend": matched.backend,
                    "source": source
                }),
            )?;
        }
    }

    Ok(())
}

mod imap_impl {
    use crate::{
        config::{Config, ImapConfig},
        db::Db,
        email::{EmailMatch, bind_matches, detect_backend_from_header, extract_match},
    };
    use anyhow::{Context, Result};
    use chrono::{Duration, Utc};
    use native_tls::TlsConnector;

    pub async fn poll_imap_if_enabled(config: &Config, db: &Db) -> Result<()> {
        let Some(imap_cfg) = &config.imap else {
            return Ok(());
        };
        if !imap_cfg.enabled {
            return Ok(());
        }
        if imap_cfg.username.trim().is_empty() || imap_cfg.password.trim().is_empty() {
            return Ok(());
        }

        let imap_cfg = imap_cfg.clone();
        let matches = tokio::task::spawn_blocking(move || poll_once_blocking(&imap_cfg))
            .await
            .context("IMAP polling task failed to join")??;

        bind_matches(db, "imap", matches)
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
            .login(&imap_cfg.username, &imap_cfg.password)
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
        oauth::{self, google::GoogleOauthProvider},
    };
    use anyhow::{Context, Result};
    use base64::{
        Engine as _,
        engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD},
    };
    use serde::Deserialize;

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

    pub async fn poll_gmail_if_enabled(config: &Config, db: &Db) -> Result<()> {
        let Some(gmail_cfg) = &config.gmail_oauth else {
            return Ok(());
        };
        if !gmail_cfg.enabled {
            return Ok(());
        }

        let Some(provider) = GoogleOauthProvider::from_config(config)? else {
            return Ok(());
        };
        let access_token = match oauth::ensure_valid_access_token(&provider).await {
            Ok(token) => token,
            Err(err) => {
                tracing::warn!(error = %err, "gmail oauth not ready; skipping gmail poll");
                return Ok(());
            }
        };

        let matches = fetch_matches(gmail_cfg, &access_token).await?;
        bind_matches(db, "gmail", matches)
    }

    async fn fetch_matches(cfg: &GmailOauthConfig, access_token: &str) -> Result<Vec<EmailMatch>> {
        let client = reqwest::Client::new();
        let query = build_unread_query(cfg);
        let max_results = cfg.max_messages_per_poll.to_string();

        let list_resp = client
            .get("https://gmail.googleapis.com/gmail/v1/users/me/messages")
            .bearer_auth(access_token)
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
            let metadata = fetch_message_metadata(&client, access_token, &msg.id).await?;
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

            let full = fetch_message_full(&client, access_token, &msg.id).await?;
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
                    mark_message_seen(&client, access_token, &msg.id).await?;
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
        let days = ((cfg.max_lookback_hours + 23) / 24).max(1);
        format!("is:unread newer_than:{days}d")
    }
}

pub async fn poll_imap_if_enabled(config: &Config, db: &Db) -> Result<()> {
    gmail_impl::poll_gmail_if_enabled(config, db).await?;
    imap_impl::poll_imap_if_enabled(config, db).await
}

#[cfg(test)]
mod tests {
    use crate::{
        config::{GmailOauthConfig, ImapConfig},
        email::detect_backend_from_header,
    };
    use chrono::{TimeZone, Utc};

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
}
