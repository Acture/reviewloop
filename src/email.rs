use crate::config::Config;
use crate::db::Db;
use anyhow::Result;

#[cfg(feature = "imap-listener")]
mod imap_impl {
    use crate::{
        config::{Config, ImapConfig},
        db::Db,
        token::{extract_review_token, extract_token_with_pattern},
        util::compute_next_poll_at,
    };
    use anyhow::{Context, Result};
    use chrono::Utc;
    use native_tls::TlsConnector;

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

        for matched in matches {
            db.record_email_token(
                &matched.token,
                &format!("imap:{}", matched.backend),
                Some("imap_unseen"),
            )?;

            if let Some(job) = db.find_latest_open_job_without_token(&matched.backend)? {
                let next_poll = compute_next_poll_at(
                    Utc::now(),
                    &config.polling.schedule_minutes,
                    0,
                    config.polling.jitter_percent,
                );
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
        let mut matches = Vec::new();

        for id in &unseen {
            let fetches = session.fetch(id.to_string(), "RFC822")?;
            for fetch in &fetches {
                if let Some(body) = fetch.body() {
                    let text = String::from_utf8_lossy(body);
                    if let Some(matched) = extract_match(&text, imap_cfg) {
                        matches.push(matched);
                    }
                }
            }

            if imap_cfg.mark_seen {
                let _ = session.store(id.to_string(), "+FLAGS (\\Seen)");
            }
        }

        let _ = session.logout();
        Ok(matches)
    }

    fn extract_match(text: &str, imap_cfg: &ImapConfig) -> Option<EmailMatch> {
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

#[cfg(feature = "imap-listener")]
pub async fn poll_imap_if_enabled(config: &Config, db: &Db) -> Result<()> {
    imap_impl::poll_imap_if_enabled(config, db).await
}

#[cfg(not(feature = "imap-listener"))]
pub async fn poll_imap_if_enabled(_config: &Config, _db: &Db) -> Result<()> {
    Ok(())
}
