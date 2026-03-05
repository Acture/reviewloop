pub mod google;

use crate::config::Config;
use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OauthTokenRecord {
    pub refresh_token: String,
    pub access_token: String,
    pub expires_at_unix: i64,
    pub scope: Option<String>,
    pub token_type: Option<String>,
    pub updated_at_unix: i64,
}

#[derive(Debug, Clone)]
pub struct DeviceCodeStart {
    pub device_code: String,
    pub user_code: String,
    pub verification_uri: String,
    pub expires_in_seconds: u64,
    pub interval_seconds: u64,
}

#[derive(Debug, Clone)]
pub struct OauthTokenResponse {
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub expires_in_seconds: u64,
    pub scope: Option<String>,
    pub token_type: Option<String>,
}

#[derive(Debug, Clone)]
pub enum DeviceCodePoll {
    Pending,
    SlowDown,
    Complete(OauthTokenResponse),
    Denied(String),
    Expired(String),
}

#[async_trait]
pub trait OauthProvider {
    fn name(&self) -> &'static str;
    fn token_store_path(&self) -> PathBuf;

    async fn start_device_flow(&self) -> Result<DeviceCodeStart>;
    async fn poll_device_flow(&self, device_code: &str) -> Result<DeviceCodePoll>;
    async fn refresh_access_token(&self, refresh_token: &str) -> Result<OauthTokenResponse>;
}

pub fn load_token_record(provider: &dyn OauthProvider) -> Result<Option<OauthTokenRecord>> {
    let path = provider.token_store_path();
    if !path.exists() {
        return Ok(None);
    }
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    let parsed: OauthTokenRecord = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse oauth token file {}", path.display()))?;
    Ok(Some(parsed))
}

pub fn save_token_record(provider: &dyn OauthProvider, token: &OauthTokenRecord) -> Result<()> {
    let path = provider.token_store_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create oauth token directory {}",
                parent.display()
            )
        })?;
    }
    let raw = serde_json::to_string_pretty(token)?;
    std::fs::write(&path, raw)
        .with_context(|| format!("failed to write oauth token file {}", path.display()))?;
    Ok(())
}

pub fn token_is_valid(token: &OauthTokenRecord, skew_seconds: i64) -> bool {
    (token.expires_at_unix - skew_seconds) > Utc::now().timestamp()
}

pub fn merge_token_response(
    old_refresh: Option<&str>,
    response: OauthTokenResponse,
) -> Result<OauthTokenRecord> {
    let refresh_token = match (response.refresh_token, old_refresh) {
        (Some(v), _) if !v.trim().is_empty() => v,
        (_, Some(existing)) if !existing.trim().is_empty() => existing.to_string(),
        _ => {
            return Err(anyhow!(
                "oauth response did not include refresh_token and no existing refresh_token was found"
            ));
        }
    };
    let now = Utc::now().timestamp();
    Ok(OauthTokenRecord {
        refresh_token,
        access_token: response.access_token,
        expires_at_unix: now + response.expires_in_seconds as i64,
        scope: response.scope,
        token_type: response.token_type,
        updated_at_unix: now,
    })
}

pub async fn ensure_valid_access_token(provider: &dyn OauthProvider) -> Result<String> {
    let Some(current) = load_token_record(provider)? else {
        return Err(anyhow!(
            "no oauth token found for provider {}; run auth login first",
            provider.name()
        ));
    };

    if token_is_valid(&current, 60) {
        return Ok(current.access_token);
    }

    let refreshed = provider
        .refresh_access_token(&current.refresh_token)
        .await?;
    let merged = merge_token_response(Some(&current.refresh_token), refreshed)?;
    save_token_record(provider, &merged)?;
    Ok(merged.access_token)
}

pub async fn run_device_login(provider: &dyn OauthProvider, _config: &Config) -> Result<PathBuf> {
    let start = provider.start_device_flow().await?;
    println!(
        "Open this URL in your browser:\n{}\n\nThen enter code: {}\n",
        start.verification_uri, start.user_code
    );

    let deadline = Utc::now().timestamp() + start.expires_in_seconds as i64;
    let mut interval = start.interval_seconds.max(3);
    let old = load_token_record(provider)?;

    loop {
        if Utc::now().timestamp() >= deadline {
            return Err(anyhow!("oauth device login timed out"));
        }

        match provider.poll_device_flow(&start.device_code).await? {
            DeviceCodePoll::Pending => {
                tokio::time::sleep(std::time::Duration::from_secs(interval)).await;
            }
            DeviceCodePoll::SlowDown => {
                interval += 5;
                tokio::time::sleep(std::time::Duration::from_secs(interval)).await;
            }
            DeviceCodePoll::Denied(message) => {
                return Err(anyhow!("oauth device login denied: {message}"));
            }
            DeviceCodePoll::Expired(message) => {
                return Err(anyhow!("oauth device code expired: {message}"));
            }
            DeviceCodePoll::Complete(response) => {
                let merged =
                    merge_token_response(old.as_ref().map(|t| t.refresh_token.as_str()), response)?;
                save_token_record(provider, &merged)?;
                return Ok(provider.token_store_path());
            }
        }
    }
}
