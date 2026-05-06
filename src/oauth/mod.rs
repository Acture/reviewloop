pub mod google;

use crate::config::Config;
use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use chrono::{DateTime, Duration, TimeZone, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use std::{
    path::{Path, PathBuf},
    process::Command,
};

const DEFAULT_ACCESS_TOKEN_REFRESH_SKEW_SECONDS: i64 = 60;

#[derive(Debug, Clone, Serialize)]
pub struct OauthTokenRecord {
    pub refresh_token: String,
    pub access_token: String,
    /// RFC3339 timestamp in the persisted JSON. `expires_at_unix` is kept for
    /// backwards compatibility with older ReviewLoop token files.
    pub expires_at: DateTime<Utc>,
    pub expires_at_unix: i64,
    pub scope: Option<String>,
    pub token_type: Option<String>,
    pub updated_at_unix: i64,
}

#[derive(Debug, Deserialize)]
struct RawOauthTokenRecord {
    refresh_token: String,
    access_token: String,
    #[serde(default)]
    expires_at: Option<DateTime<Utc>>,
    #[serde(default)]
    expires_at_unix: Option<i64>,
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    token_type: Option<String>,
    #[serde(default)]
    updated_at_unix: Option<i64>,
}

impl<'de> Deserialize<'de> for OauthTokenRecord {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawOauthTokenRecord::deserialize(deserializer)?;
        let now = Utc::now();
        let expires_at = raw
            .expires_at
            .or_else(|| raw.expires_at_unix.and_then(unix_timestamp_to_datetime))
            .unwrap_or_else(|| now + Duration::hours(1));

        Ok(Self {
            refresh_token: raw.refresh_token,
            access_token: raw.access_token,
            expires_at_unix: expires_at.timestamp(),
            expires_at,
            scope: raw.scope,
            token_type: raw.token_type,
            updated_at_unix: raw.updated_at_unix.unwrap_or_else(|| now.timestamp()),
        })
    }
}

fn unix_timestamp_to_datetime(timestamp: i64) -> Option<DateTime<Utc>> {
    Utc.timestamp_opt(timestamp, 0).single()
}

#[derive(Debug, Clone)]
pub struct DeviceCodeStart {
    pub device_code: String,
    pub user_code: String,
    pub verification_uri: String,
    pub verification_uri_complete: Option<String>,
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
    let raw = std::fs::read_to_string(&path).map_err(|e| {
        let ctx = if e.kind() == std::io::ErrorKind::PermissionDenied {
            format!(
                "failed to read {}; ensure the file is owned by your user — if you previously \
                 ran reviewloop with sudo, run `sudo chown $(whoami) {}` or remove the file and \
                 re-init",
                path.display(),
                path.display()
            )
        } else {
            format!("failed to read {}", path.display())
        };
        anyhow::Error::from(e).context(ctx)
    })?;
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
    let tmp_path = temporary_token_path(&path);
    let write_result = (|| -> Result<()> {
        write_token_file(&tmp_path, raw.as_bytes())?;
        std::fs::rename(&tmp_path, &path).with_context(|| {
            format!(
                "failed to atomically replace oauth token file {}",
                path.display()
            )
        })?;
        #[cfg(unix)]
        sync_token_directory(&path)?;
        Ok(())
    })();

    if write_result.is_err() {
        let _ = std::fs::remove_file(&tmp_path);
    }
    write_result
}

fn temporary_token_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .map(|v| v.to_string_lossy())
        .unwrap_or_else(|| "oauth_token".into());
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    path.with_file_name(format!(".{file_name}.{}.{}.tmp", std::process::id(), nonce))
}

fn write_token_file(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;

    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }

    let mut f = options
        .open(path)
        .with_context(|| format!("failed to open oauth token file {}", path.display()))?;
    f.write_all(bytes)
        .with_context(|| format!("failed to write oauth token file {}", path.display()))?;
    f.sync_all()
        .with_context(|| format!("failed to sync oauth token file {}", path.display()))?;
    Ok(())
}

#[cfg(unix)]
fn sync_token_directory(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::File::open(parent)
            .and_then(|f| f.sync_all())
            .with_context(|| {
                format!("failed to sync oauth token directory {}", parent.display())
            })?;
    }
    Ok(())
}

pub fn token_is_valid(token: &OauthTokenRecord, skew_seconds: i64) -> bool {
    token.expires_at > Utc::now() + Duration::seconds(skew_seconds)
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
    let now = Utc::now();
    let expires_at = now + Duration::seconds(response.expires_in_seconds as i64);
    Ok(OauthTokenRecord {
        refresh_token,
        access_token: response.access_token,
        expires_at,
        expires_at_unix: expires_at.timestamp(),
        scope: response.scope,
        token_type: response.token_type,
        updated_at_unix: now.timestamp(),
    })
}

pub async fn ensure_valid_access_token(provider: &dyn OauthProvider) -> Result<String> {
    ensure_valid_access_token_with_skew(provider, DEFAULT_ACCESS_TOKEN_REFRESH_SKEW_SECONDS).await
}

pub async fn ensure_valid_access_token_with_skew(
    provider: &dyn OauthProvider,
    skew_seconds: i64,
) -> Result<String> {
    let Some(current) = load_token_record(provider)? else {
        return Err(anyhow!(
            "no oauth token found for provider {}; run auth login first",
            provider.name()
        ));
    };

    if token_is_valid(&current, skew_seconds) {
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
    let open_url = start
        .verification_uri_complete
        .as_deref()
        .unwrap_or(start.verification_uri.as_str());
    let browser_opened = open_browser_url(open_url);
    if browser_opened {
        println!("Opened browser for {} login.", provider.name());
    }
    println!(
        "If browser did not open, visit:\n{}\n\nDevice code: {}\n",
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

pub(crate) fn open_browser_url(url: &str) -> bool {
    #[cfg(target_os = "macos")]
    {
        if Command::new("open")
            .arg(url)
            .status()
            .is_ok_and(|s| s.success())
        {
            return true;
        }
    }

    #[cfg(target_os = "windows")]
    {
        if Command::new("cmd")
            .args(["/C", "start", "", url])
            .status()
            .is_ok_and(|s| s.success())
        {
            return true;
        }
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    {
        if Command::new("xdg-open")
            .arg(url)
            .status()
            .is_ok_and(|s| s.success())
        {
            return true;
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn token_file_has_mode_0600() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let token_path = tmp.path().join("google_token.json");

        struct MockProvider {
            path: std::path::PathBuf,
        }
        #[async_trait::async_trait]
        impl OauthProvider for MockProvider {
            fn name(&self) -> &'static str {
                "mock"
            }
            fn token_store_path(&self) -> std::path::PathBuf {
                self.path.clone()
            }
            async fn start_device_flow(&self) -> anyhow::Result<DeviceCodeStart> {
                unimplemented!()
            }
            async fn poll_device_flow(&self, _: &str) -> anyhow::Result<DeviceCodePoll> {
                unimplemented!()
            }
            async fn refresh_access_token(&self, _: &str) -> anyhow::Result<OauthTokenResponse> {
                unimplemented!()
            }
        }

        let provider = MockProvider {
            path: token_path.clone(),
        };
        let expires_at = unix_timestamp_to_datetime(9999999999).expect("valid timestamp");
        let token = OauthTokenRecord {
            refresh_token: "rt".to_string(),
            access_token: "at".to_string(),
            expires_at,
            expires_at_unix: expires_at.timestamp(),
            scope: None,
            token_type: None,
            updated_at_unix: 0,
        };
        save_token_record(&provider, &token).expect("save_token_record");

        let metadata = std::fs::metadata(&token_path).expect("metadata");
        let mode = metadata.permissions().mode() & 0o777;
        assert_eq!(
            mode, 0o600,
            "token file should have mode 0600, got {:o}",
            mode
        );
    }

    #[test]
    fn legacy_token_without_rfc3339_expiry_uses_unix_expiry() {
        let legacy_expires_at = Utc::now() + Duration::minutes(42);
        let raw = serde_json::json!({
            "refresh_token": "rt",
            "access_token": "at",
            "expires_at_unix": legacy_expires_at.timestamp(),
            "scope": null,
            "token_type": "bearer",
            "updated_at_unix": 1,
        });

        let parsed: OauthTokenRecord = serde_json::from_value(raw).expect("legacy token parses");

        assert_eq!(parsed.expires_at.timestamp(), legacy_expires_at.timestamp());
        assert_eq!(parsed.expires_at_unix, legacy_expires_at.timestamp());
    }
}
