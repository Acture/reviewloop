use crate::config::{Config, GmailOauthConfig};
use crate::oauth::{
    DeviceCodePoll, DeviceCodeStart, OauthProvider, OauthTokenResponse, load_token_record,
    merge_token_response, open_browser_url, save_token_record,
};
use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use oauth2::basic::{BasicClient, BasicTokenType};
use oauth2::{
    AuthType, AuthUrl, AuthorizationCode, ClientId, ClientSecret, CsrfToken,
    DeviceAuthorizationUrl, DeviceCodeErrorResponseType, PkceCodeChallenge, PkceCodeVerifier,
    RedirectUrl, RefreshToken, RequestTokenError, Scope, StandardDeviceAuthorizationResponse,
    TokenResponse, TokenUrl,
};
use std::{
    collections::HashMap,
    env,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

const DEFAULT_GOOGLE_CLIENT_ID: &str =
    "159112762522-82ces4lrs8hodbl79gu9usvmvs4lkmnr.apps.googleusercontent.com";
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
        Self::from_config_with_mode(config, false)
    }

    pub fn from_config_for_login(config: &Config) -> Result<Option<Self>> {
        Self::from_config_with_mode(config, true)
    }

    fn from_config_with_mode(config: &Config, allow_disabled: bool) -> Result<Option<Self>> {
        let Some(cfg) = &config.gmail_oauth else {
            return Ok(None);
        };
        if !cfg.enabled && !allow_disabled {
            return Ok(None);
        }
        let mut resolved = cfg.clone();
        if resolved.client_id.trim().is_empty() {
            resolved.client_id = env::var("REVIEWLOOP_GMAIL_CLIENT_ID")
                .unwrap_or_else(|_| DEFAULT_GOOGLE_CLIENT_ID.to_string());
        }
        if resolved.client_secret.trim().is_empty() {
            resolved.client_secret = env::var("REVIEWLOOP_GMAIL_CLIENT_SECRET").unwrap_or_default();
        }
        if resolved.client_id.trim().is_empty() {
            return Ok(None);
        }
        let token_path = if let Some(path) = &resolved.token_store_path {
            PathBuf::from(path)
        } else {
            config.state_dir().join("oauth").join("google_token.json")
        };

        Ok(Some(Self {
            cfg: resolved,
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

    pub async fn run_browser_pkce_login(&self) -> Result<PathBuf> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .context("failed to bind local oauth callback listener")?;
        let callback_addr = listener
            .local_addr()
            .context("failed to resolve local oauth callback listener address")?;
        let redirect_uri = format!("http://{}/oauth2/callback", callback_addr);
        let redirect = RedirectUrl::new(redirect_uri.clone()).context("invalid redirect url")?;
        let http_client = Self::http_client()?;
        let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();

        let (auth_url, csrf_token) = if self.cfg.client_secret.trim().is_empty() {
            BasicClient::new(ClientId::new(self.cfg.client_id.clone()))
                .set_auth_uri(
                    AuthUrl::new(AUTH_URL.to_string()).context("invalid google auth url")?,
                )
                .set_token_uri(
                    TokenUrl::new(TOKEN_URL.to_string()).context("invalid google token url")?,
                )
                .set_redirect_uri(redirect.clone())
                .set_auth_type(AuthType::RequestBody)
                .authorize_url(CsrfToken::new_random)
                .add_scope(Scope::new(self.scope().to_string()))
                .set_pkce_challenge(pkce_challenge.clone())
                .add_extra_param("access_type", "offline")
                .add_extra_param("prompt", "consent")
                .url()
        } else {
            BasicClient::new(ClientId::new(self.cfg.client_id.clone()))
                .set_client_secret(ClientSecret::new(self.cfg.client_secret.clone()))
                .set_auth_uri(
                    AuthUrl::new(AUTH_URL.to_string()).context("invalid google auth url")?,
                )
                .set_token_uri(
                    TokenUrl::new(TOKEN_URL.to_string()).context("invalid google token url")?,
                )
                .set_redirect_uri(redirect)
                .set_auth_type(AuthType::RequestBody)
                .authorize_url(CsrfToken::new_random)
                .add_scope(Scope::new(self.scope().to_string()))
                .set_pkce_challenge(pkce_challenge.clone())
                .add_extra_param("access_type", "offline")
                .add_extra_param("prompt", "consent")
                .url()
        };

        if open_browser_url(auth_url.as_str()) {
            println!("Opened browser for google login.");
        } else {
            println!("Open this URL in your browser:\n{}\n", auth_url);
        }
        println!("Waiting for OAuth callback on {redirect_uri} ...");

        let auth_code = Self::wait_for_callback_code(listener, csrf_token.secret()).await?;
        let old = load_token_record(self)?;
        let pkce_verifier_secret = pkce_verifier.secret().to_string();

        let token = if self.cfg.client_secret.trim().is_empty() {
            BasicClient::new(ClientId::new(self.cfg.client_id.clone()))
                .set_auth_uri(
                    AuthUrl::new(AUTH_URL.to_string()).context("invalid google auth url")?,
                )
                .set_token_uri(
                    TokenUrl::new(TOKEN_URL.to_string()).context("invalid google token url")?,
                )
                .set_redirect_uri(
                    RedirectUrl::new(redirect_uri.clone()).context("invalid redirect url")?,
                )
                .set_auth_type(AuthType::RequestBody)
                .exchange_code(AuthorizationCode::new(auth_code))
                .set_pkce_verifier(PkceCodeVerifier::new(pkce_verifier_secret.clone()))
                .request_async(&http_client)
                .await
                .context("failed to exchange authorization code")?
        } else {
            BasicClient::new(ClientId::new(self.cfg.client_id.clone()))
                .set_client_secret(ClientSecret::new(self.cfg.client_secret.clone()))
                .set_auth_uri(
                    AuthUrl::new(AUTH_URL.to_string()).context("invalid google auth url")?,
                )
                .set_token_uri(
                    TokenUrl::new(TOKEN_URL.to_string()).context("invalid google token url")?,
                )
                .set_redirect_uri(RedirectUrl::new(redirect_uri).context("invalid redirect url")?)
                .set_auth_type(AuthType::RequestBody)
                .exchange_code(AuthorizationCode::new(auth_code))
                .set_pkce_verifier(PkceCodeVerifier::new(pkce_verifier_secret))
                .request_async(&http_client)
                .await
                .context("failed to exchange authorization code")?
        };

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
        let response = OauthTokenResponse {
            access_token: token.access_token().secret().to_string(),
            refresh_token: token.refresh_token().map(|t| t.secret().to_string()),
            expires_in_seconds: token.expires_in().map(|d| d.as_secs()).unwrap_or(3600),
            scope,
            token_type,
        };
        let merged =
            merge_token_response(old.as_ref().map(|t| t.refresh_token.as_str()), response)?;
        save_token_record(self, &merged)?;
        Ok(self.token_store_path())
    }

    async fn wait_for_callback_code(listener: TcpListener, expected_state: &str) -> Result<String> {
        let (mut socket, _) = tokio::time::timeout(Duration::from_secs(300), listener.accept())
            .await
            .context("oauth callback timed out waiting for browser redirect")?
            .context("failed to accept oauth callback connection")?;

        let mut buf = vec![0u8; 8192];
        let n = socket
            .read(&mut buf)
            .await
            .context("failed to read oauth callback request")?;
        let request = String::from_utf8_lossy(&buf[..n]);
        let first_line = request
            .lines()
            .next()
            .ok_or_else(|| anyhow!("empty oauth callback request"))?;
        let path = first_line
            .strip_prefix("GET ")
            .and_then(|v| v.split(" HTTP/").next())
            .ok_or_else(|| anyhow!("invalid oauth callback request line"))?;

        let url = reqwest::Url::parse(&format!("http://localhost{path}"))
            .context("failed to parse oauth callback url")?;
        let mut code: Option<String> = None;
        let mut state: Option<String> = None;
        let mut err: Option<String> = None;
        for (k, v) in url.query_pairs() {
            match k.as_ref() {
                "code" => code = Some(v.into_owned()),
                "state" => state = Some(v.into_owned()),
                "error" => err = Some(v.into_owned()),
                _ => {}
            }
        }

        if let Some(error) = err {
            let _ = socket
                .write_all(
                    b"HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\n\r\nOAuth login failed. You can close this tab.",
                )
                .await;
            return Err(anyhow!("oauth authorization failed: {error}"));
        }

        if state.as_deref() != Some(expected_state) {
            let _ = socket
                .write_all(
                    b"HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\n\r\nOAuth state mismatch. You can close this tab.",
                )
                .await;
            return Err(anyhow!("oauth state mismatch"));
        }

        let Some(code) = code else {
            let _ = socket
                .write_all(
                    b"HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\n\r\nMissing authorization code. You can close this tab.",
                )
                .await;
            return Err(anyhow!("oauth callback missing authorization code"));
        };

        let _ = socket
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\nReviewLoop login completed. You can return to terminal.",
            )
            .await;
        Ok(code)
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
        let http_client = Self::http_client()?;
        let auth_url = AuthUrl::new(AUTH_URL.to_string()).context("invalid google auth url")?;
        let token_url = TokenUrl::new(TOKEN_URL.to_string()).context("invalid google token url")?;
        let device_auth_url = DeviceAuthorizationUrl::new(DEVICE_CODE_URL.to_string())
            .context("invalid google device code url")?;
        let details: StandardDeviceAuthorizationResponse =
            if self.cfg.client_secret.trim().is_empty() {
                BasicClient::new(ClientId::new(self.cfg.client_id.clone()))
                    .set_auth_uri(auth_url.clone())
                    .set_token_uri(token_url.clone())
                    .set_device_authorization_url(device_auth_url.clone())
                    .set_auth_type(AuthType::RequestBody)
                    .exchange_device_code()
                    .add_scope(Scope::new(self.scope().to_string()))
                    .request_async(&http_client)
                    .await
                    .context("failed to request google device code")?
            } else {
                BasicClient::new(ClientId::new(self.cfg.client_id.clone()))
                    .set_client_secret(ClientSecret::new(self.cfg.client_secret.clone()))
                    .set_auth_uri(auth_url)
                    .set_token_uri(token_url)
                    .set_device_authorization_url(device_auth_url)
                    .set_auth_type(AuthType::RequestBody)
                    .exchange_device_code()
                    .add_scope(Scope::new(self.scope().to_string()))
                    .request_async(&http_client)
                    .await
                    .context("failed to request google device code")?
            };

        let device_code = details.device_code().secret().to_string();
        self.pending
            .lock()
            .map_err(|_| anyhow!("oauth pending session mutex poisoned"))?
            .insert(device_code.clone(), details.clone());

        Ok(DeviceCodeStart {
            device_code,
            user_code: details.user_code().secret().to_string(),
            verification_uri: details.verification_uri().to_string(),
            verification_uri_complete: details
                .verification_uri_complete()
                .map(|v| v.secret().to_string()),
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

        let http_client = Self::http_client()?;
        let auth_url = AuthUrl::new(AUTH_URL.to_string()).context("invalid google auth url")?;
        let token_url = TokenUrl::new(TOKEN_URL.to_string()).context("invalid google token url")?;
        let device_auth_url = DeviceAuthorizationUrl::new(DEVICE_CODE_URL.to_string())
            .context("invalid google device code url")?;
        let token_result = if self.cfg.client_secret.trim().is_empty() {
            BasicClient::new(ClientId::new(self.cfg.client_id.clone()))
                .set_auth_uri(auth_url.clone())
                .set_token_uri(token_url.clone())
                .set_device_authorization_url(device_auth_url.clone())
                .set_auth_type(AuthType::RequestBody)
                .exchange_device_access_token(&details)
                .request_async(&http_client, tokio::time::sleep, None)
                .await
        } else {
            BasicClient::new(ClientId::new(self.cfg.client_id.clone()))
                .set_client_secret(ClientSecret::new(self.cfg.client_secret.clone()))
                .set_auth_uri(auth_url)
                .set_token_uri(token_url)
                .set_device_authorization_url(device_auth_url)
                .set_auth_type(AuthType::RequestBody)
                .exchange_device_access_token(&details)
                .request_async(&http_client, tokio::time::sleep, None)
                .await
        };

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
        let http_client = Self::http_client()?;
        let auth_url = AuthUrl::new(AUTH_URL.to_string()).context("invalid google auth url")?;
        let token_url = TokenUrl::new(TOKEN_URL.to_string()).context("invalid google token url")?;
        let device_auth_url = DeviceAuthorizationUrl::new(DEVICE_CODE_URL.to_string())
            .context("invalid google device code url")?;
        let token = if self.cfg.client_secret.trim().is_empty() {
            BasicClient::new(ClientId::new(self.cfg.client_id.clone()))
                .set_auth_uri(auth_url.clone())
                .set_token_uri(token_url.clone())
                .set_device_authorization_url(device_auth_url.clone())
                .set_auth_type(AuthType::RequestBody)
                .exchange_refresh_token(&RefreshToken::new(refresh_token.to_string()))
                .request_async(&http_client)
                .await
                .context("failed to refresh google access token")?
        } else {
            BasicClient::new(ClientId::new(self.cfg.client_id.clone()))
                .set_client_secret(ClientSecret::new(self.cfg.client_secret.clone()))
                .set_auth_uri(auth_url)
                .set_token_uri(token_url)
                .set_device_authorization_url(device_auth_url)
                .set_auth_type(AuthType::RequestBody)
                .exchange_refresh_token(&RefreshToken::new(refresh_token.to_string()))
                .request_async(&http_client)
                .await
                .context("failed to refresh google access token")?
        };

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
