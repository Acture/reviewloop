use crate::config::Config;
use crate::db::Db;
use anyhow::Result;
use regex::Regex;

fn detect_backend_from_header(
    header_text: &str,
    config: &crate::config::ImapConfig,
) -> Option<String> {
    for (backend, pattern) in &config.backend_header_patterns {
        if let Ok(re) = Regex::new(pattern)
            && re.is_match(header_text)
        {
            return Some(backend.clone());
        }
    }
    None
}

mod imap_impl {
    use crate::{
        config::{Config, ImapConfig},
        db::Db,
        email::detect_backend_from_header,
        token::{extract_review_token, extract_token_with_pattern},
    };
    use anyhow::{Context, Result};
    use chrono::Utc;
    use native_tls::TlsConnector;
    use std::collections::HashSet;

    #[derive(Debug, Clone)]
    struct EmailMatch {
        backend: String,
        token: String,
    }

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

        let mut seen_tokens = HashSet::new();
        for matched in matches {
            if !seen_tokens.insert(matched.token.clone()) {
                continue;
            }

            db.record_email_token(
                &matched.token,
                &format!("imap:{}", matched.backend),
                Some("imap_unseen"),
            )?;

            if let Some(existing_job) = db.find_job_by_token(&matched.token)? {
                db.add_event(
                    Some(&existing_job.id),
                    "imap_token_already_bound",
                    serde_json::json!({
                        "token": matched.token,
                        "backend": matched.backend,
                        "source": "imap"
                    }),
                )?;
                continue;
            }

            if let Some(job) = db.find_latest_open_job_without_token(&matched.backend)? {
                // IMAP token match should trigger immediate fetch in the same daemon tick.
                let next_poll = Utc::now();
                db.attach_token_to_job(&job.id, &matched.token, next_poll)?;
                db.add_event(
                    Some(&job.id),
                    "imap_token_attached",
                    serde_json::json!({
                        "token": matched.token,
                        "backend": matched.backend,
                        "source": "imap"
                    }),
                )?;
            }
        }

        Ok(())
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

        let unseen = session.search("UNSEEN")?;
        let mut unseen_ids: Vec<u32> = unseen.into_iter().collect();
        // Prefer newest messages first so the latest token maps to the latest open job.
        unseen_ids.sort_unstable_by(|a, b| b.cmp(a));
        let mut matches = Vec::new();

        for id in unseen_ids {
            let backend_hint = if imap_cfg.header_first {
                fetch_header_text(&mut session, id)
                    .and_then(|headers| detect_backend_from_header(&headers, imap_cfg))
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
                    if let Some(matched) = extract_match(&text, imap_cfg, backend_hint.as_deref()) {
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

    fn extract_match(
        text: &str,
        imap_cfg: &ImapConfig,
        backend_hint: Option<&str>,
    ) -> Option<EmailMatch> {
        if let Some(backend) = backend_hint {
            if let Some(pattern) = imap_cfg.backend_patterns.get(backend)
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

        for (backend, pattern) in &imap_cfg.backend_patterns {
            if let Some(token) = extract_token_with_pattern(text, pattern) {
                return Some(EmailMatch {
                    backend: backend.clone(),
                    token,
                });
            }
        }

        // Compatibility fallback for old configs with no override map.
        extract_review_token(text).map(|token| EmailMatch {
            backend: "stanford".to_string(),
            token,
        })
    }
}

pub async fn poll_imap_if_enabled(config: &Config, db: &Db) -> Result<()> {
    imap_impl::poll_imap_if_enabled(config, db).await
}

#[cfg(test)]
mod tests {
    use crate::config::ImapConfig;

    #[test]
    fn header_match_detects_stanford_sender() {
        let cfg = ImapConfig::default();
        let header =
            "From: Stanford Agentic Reviewer <review@mail.paperreview.ai>\r\nSubject: hello\r\n";
        let backend = super::detect_backend_from_header(header, &cfg);
        assert_eq!(backend.as_deref(), Some("stanford"));
    }

    #[test]
    fn header_match_returns_none_for_unrelated_mail() {
        let cfg = ImapConfig::default();
        let header = "From: notifications@github.com\r\nSubject: PR updated\r\n";
        let backend = super::detect_backend_from_header(header, &cfg);
        assert!(backend.is_none());
    }
}
