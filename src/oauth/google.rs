use crate::config::{Config, GmailOauthConfig};
use crate::oauth::{DeviceCodePoll, DeviceCodeStart, OauthProvider, OauthTokenResponse};
use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use oauth2::basic::{BasicClient, BasicTokenType};
use oauth2::{
    AuthType, AuthUrl, ClientId, ClientSecret, DeviceAuthorizationUrl, DeviceCodeErrorResponseType,
    RefreshToken, RequestTokenError, Scope, StandardDeviceAuthorizationResponse, TokenResponse,
    TokenUrl,
};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
};

const AUTH_URL: &str = "https://accounts.google.com/o/oauth2/v2/auth";
const TOKEN_URL: &str = "https://www.googleapis.com/oauth2/v3/token";
const DEVICE_CODE_URL: &str = "https://oauth2.googleapis.com/device/code";
const SCOPE_READONLY: &str = "https://www.googleapis.com/auth/gmail.readonly";
const SCOPE_MODIFY: &str = "https://www.googleapis.com/auth/gmail.modify";

#[derive(Clone)]
pub struct GoogleOauthProvider {
    cfg: GmailOauthConfig,
    token_path: PathBuf,
    pending: Arc<Mutex<HashMap<String, StandardDeviceAuthorizationResponse>>>,
}

impl GoogleOauthProvider {
    pub fn from_config(config: &Config) -> Result<Option<Self>> {
        let Some(cfg) = &config.gmail_oauth else {
            return Ok(None);
        };
        if !cfg.enabled {
            return Ok(None);
        }
        if cfg.client_id.trim().is_empty() || cfg.client_secret.trim().is_empty() {
            return Ok(None);
        }
        let token_path = if let Some(path) = &cfg.token_store_path {
            PathBuf::from(path)
        } else {
            config.state_dir().join("oauth").join("google_token.json")
        };

        Ok(Some(Self {
            cfg: cfg.clone(),
            token_path,
            pending: Arc::new(Mutex::new(HashMap::new())),
        }))
    }

    fn scope(&self) -> &'static str {
        if self.cfg.mark_seen {
            SCOPE_MODIFY
        } else {
            SCOPE_READONLY
        }
    }

    fn http_client() -> Result<reqwest::Client> {
        reqwest::ClientBuilder::new()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .context("failed to build oauth http client")
    }
}

#[async_trait]
impl OauthProvider for GoogleOauthProvider {
    fn name(&self) -> &'static str {
        "google"
    }

    fn token_store_path(&self) -> PathBuf {
        self.token_path.clone()
    }

    async fn start_device_flow(&self) -> Result<DeviceCodeStart> {
        let auth_url = AuthUrl::new(AUTH_URL.to_string()).context("invalid google auth url")?;
        let token_url = TokenUrl::new(TOKEN_URL.to_string()).context("invalid google token url")?;
        let device_auth_url = DeviceAuthorizationUrl::new(DEVICE_CODE_URL.to_string())
            .context("invalid google device code url")?;
        let client = BasicClient::new(ClientId::new(self.cfg.client_id.clone()))
            .set_client_secret(ClientSecret::new(self.cfg.client_secret.clone()))
            .set_auth_uri(auth_url)
            .set_token_uri(token_url)
            .set_device_authorization_url(device_auth_url)
            .set_auth_type(AuthType::RequestBody);
        let http_client = Self::http_client()?;
        let details: StandardDeviceAuthorizationResponse = client
            .exchange_device_code()
            .add_scope(Scope::new(self.scope().to_string()))
            .request_async(&http_client)
            .await
            .context("failed to request google device code")?;

        let device_code = details.device_code().secret().to_string();
        self.pending
            .lock()
            .map_err(|_| anyhow!("oauth pending session mutex poisoned"))?
            .insert(device_code.clone(), details.clone());

        Ok(DeviceCodeStart {
            device_code,
            user_code: details.user_code().secret().to_string(),
            verification_uri: details.verification_uri().to_string(),
            expires_in_seconds: details.expires_in().as_secs(),
            interval_seconds: details.interval().as_secs(),
        })
    }

    async fn poll_device_flow(&self, device_code: &str) -> Result<DeviceCodePoll> {
        let details = self
            .pending
            .lock()
            .map_err(|_| anyhow!("oauth pending session mutex poisoned"))?
            .get(device_code)
            .cloned()
            .ok_or_else(|| anyhow!("missing pending device session for code"))?;

        let auth_url = AuthUrl::new(AUTH_URL.to_string()).context("invalid google auth url")?;
        let token_url = TokenUrl::new(TOKEN_URL.to_string()).context("invalid google token url")?;
        let device_auth_url = DeviceAuthorizationUrl::new(DEVICE_CODE_URL.to_string())
            .context("invalid google device code url")?;
        let client = BasicClient::new(ClientId::new(self.cfg.client_id.clone()))
            .set_client_secret(ClientSecret::new(self.cfg.client_secret.clone()))
            .set_auth_uri(auth_url)
            .set_token_uri(token_url)
            .set_device_authorization_url(device_auth_url)
            .set_auth_type(AuthType::RequestBody);
        let http_client = Self::http_client()?;
        let token_result = client
            .exchange_device_access_token(&details)
            .request_async(&http_client, tokio::time::sleep, None)
            .await;

        match token_result {
            Ok(token) => {
                let _ = self
                    .pending
                    .lock()
                    .map_err(|_| anyhow!("oauth pending session mutex poisoned"))?
                    .remove(device_code);
                let scope = token.scopes().map(|scopes| {
                    scopes
                        .iter()
                        .map(|s| s.as_ref())
                        .collect::<Vec<_>>()
                        .join(" ")
                });
                let token_type = match token.token_type() {
                    BasicTokenType::Bearer => Some("bearer".to_string()),
                    BasicTokenType::Extension(v) => Some(v.clone()),
                    _ => None,
                };

                Ok(DeviceCodePoll::Complete(OauthTokenResponse {
                    access_token: token.access_token().secret().to_string(),
                    refresh_token: token.refresh_token().map(|t| t.secret().to_string()),
                    expires_in_seconds: token.expires_in().map(|d| d.as_secs()).unwrap_or(3600),
                    scope,
                    token_type,
                }))
            }
            Err(RequestTokenError::ServerResponse(err)) => {
                let detail = err
                    .error_description()
                    .cloned()
                    .unwrap_or_else(|| format!("google oauth error: {}", err.error()));
                let poll = match err.error() {
                    DeviceCodeErrorResponseType::AuthorizationPending => DeviceCodePoll::Pending,
                    DeviceCodeErrorResponseType::SlowDown => DeviceCodePoll::SlowDown,
                    DeviceCodeErrorResponseType::AccessDenied => DeviceCodePoll::Denied(detail),
                    DeviceCodeErrorResponseType::ExpiredToken => DeviceCodePoll::Expired(detail),
                    _ => DeviceCodePoll::Denied(detail),
                };
                match poll {
                    DeviceCodePoll::Denied(_) | DeviceCodePoll::Expired(_) => {
                        let _ = self
                            .pending
                            .lock()
                            .map_err(|_| anyhow!("oauth pending session mutex poisoned"))?
                            .remove(device_code);
                    }
                    _ => {}
                }
                Ok(poll)
            }
            Err(err) => Err(anyhow!("google oauth device polling failed: {err}")),
        }
    }

    async fn refresh_access_token(&self, refresh_token: &str) -> Result<OauthTokenResponse> {
        let auth_url = AuthUrl::new(AUTH_URL.to_string()).context("invalid google auth url")?;
        let token_url = TokenUrl::new(TOKEN_URL.to_string()).context("invalid google token url")?;
        let device_auth_url = DeviceAuthorizationUrl::new(DEVICE_CODE_URL.to_string())
            .context("invalid google device code url")?;
        let client = BasicClient::new(ClientId::new(self.cfg.client_id.clone()))
            .set_client_secret(ClientSecret::new(self.cfg.client_secret.clone()))
            .set_auth_uri(auth_url)
            .set_token_uri(token_url)
            .set_device_authorization_url(device_auth_url)
            .set_auth_type(AuthType::RequestBody);
        let http_client = Self::http_client()?;
        let token = client
            .exchange_refresh_token(&RefreshToken::new(refresh_token.to_string()))
            .request_async(&http_client)
            .await
            .context("failed to refresh google access token")?;

        let scope = token.scopes().map(|scopes| {
            scopes
                .iter()
                .map(|s| s.as_ref())
                .collect::<Vec<_>>()
                .join(" ")
        });
        let token_type = match token.token_type() {
            BasicTokenType::Bearer => Some("bearer".to_string()),
            BasicTokenType::Extension(v) => Some(v.clone()),
            _ => None,
        };

        Ok(OauthTokenResponse {
            access_token: token.access_token().secret().to_string(),
            refresh_token: token.refresh_token().map(|t| t.secret().to_string()),
            expires_in_seconds: token.expires_in().map(|d| d.as_secs()).unwrap_or(3600),
            scope,
            token_type,
        })
    }
}
