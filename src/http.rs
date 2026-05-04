//! Centralized factory for outbound HTTP clients.
//!
//! All `reqwest::Client` construction in the worker / backend / oauth / email
//! modules goes through [`build_client`] or [`build_reqwest_client`] so proxy
//! configuration applies uniformly.
//!
//! # Proxy pool strategy
//!
//! [`reqwest-proxy-pool`](https://crates.io/crates/reqwest-proxy-pool) 0.4 only
//! supports proxy lists fetched from remote URLs (`.sources(vec![url, ...])`);
//! it has no API for a static user-supplied list.  Therefore this module
//! implements **option (b)**: a lightweight custom
//! [`reqwest_middleware::Middleware`] that round-robins across pre-built
//! `reqwest::Client` instances (one per proxy URL).  This keeps the rotation
//! logic inside the library layer (`reqwest-middleware`) and out of
//! application code, while still giving the user full control over the proxy
//! list via `core.proxies`.
//!
//! Cooldown / health-check is deferred to a future phase when upstream adds a
//! static-list API.

use crate::config::Config;
use anyhow::{Context, Result};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

/// Round-robin proxy selection middleware.
///
/// Pre-builds one `reqwest::Client` per configured proxy URL and cycles
/// through them with an atomic counter.  When [`handle`] is called it picks
/// the next client and executes the already-built `reqwest::Request` through
/// it, bypassing the inner `ClientWithMiddleware` client entirely.
struct RoundRobinProxyMiddleware {
    /// One client per proxy URL, built at construction time.
    clients: Vec<reqwest::Client>,
    /// Monotonically increasing counter; modulo `clients.len()` gives index.
    counter: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl reqwest_middleware::Middleware for RoundRobinProxyMiddleware {
    async fn handle(
        &self,
        req: reqwest::Request,
        _extensions: &mut http::Extensions,
        _next: reqwest_middleware::Next<'_>,
    ) -> reqwest_middleware::Result<reqwest::Response> {
        // `clients` is never empty when this middleware is installed.
        let idx = self.counter.fetch_add(1, Ordering::Relaxed) % self.clients.len();
        self.clients[idx]
            .execute(req)
            .await
            .map_err(reqwest_middleware::Error::Reqwest)
    }
}

/// Build an outbound HTTP client with proxy pool middleware when proxies are
/// configured.
///
/// When `config.core.proxies` is empty, returns a plain
/// `ClientWithMiddleware` with no middleware — behaviour identical to a bare
/// `reqwest::Client::new()`.
///
/// When proxies are configured, installs [`RoundRobinProxyMiddleware`] so
/// every request cycles through the proxy list.  Only the count is logged;
/// individual proxy URLs are never emitted to avoid leaking embedded
/// credentials.
pub fn build_client(config: &Config) -> Result<ClientWithMiddleware> {
    if config.core.proxies.is_empty() {
        return Ok(ClientBuilder::new(reqwest::Client::new()).build());
    }

    tracing::info!(
        count = config.core.proxies.len(),
        "outbound HTTP client: enabling round-robin proxy pool"
    );

    let proxy_clients = config
        .core
        .proxies
        .iter()
        .enumerate()
        .map(|(i, url)| {
            let proxy = reqwest::Proxy::all(url)
                .with_context(|| format!("invalid proxy URL at index {i}"))?;
            reqwest::Client::builder()
                .proxy(proxy)
                .build()
                .with_context(|| format!("failed to build client for proxy at index {i}"))
        })
        .collect::<Result<Vec<_>>>()?;

    let middleware = RoundRobinProxyMiddleware {
        clients: proxy_clients,
        counter: Arc::new(AtomicUsize::new(0)),
    };

    Ok(ClientBuilder::new(reqwest::Client::new())
        .with(middleware)
        .build())
}

/// Build a plain `reqwest::Client` with the first configured proxy applied.
///
/// Used for OAuth2 token exchange flows that require a bare `reqwest::Client`
/// (the `oauth2` crate's `AsyncHttpClient` trait is implemented for
/// `reqwest::Client`, not `ClientWithMiddleware`).  When no proxies are
/// configured this is equivalent to `reqwest::Client::new()`.
pub fn build_reqwest_client(config: &Config) -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder();
    if let Some(proxy_url) = config.core.proxies.first() {
        let proxy = reqwest::Proxy::all(proxy_url).context("invalid proxy URL at index 0")?;
        builder = builder.proxy(proxy);
    }
    builder.build().context("failed to build reqwest client")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    #[test]
    fn build_client_no_proxies_succeeds() {
        let config = Config::default();
        assert!(config.core.proxies.is_empty());
        let client = build_client(&config).expect("build_client with no proxies");
        // Verify it is usable: just assert the type compiles and builds.
        drop(client);
    }

    #[test]
    fn build_reqwest_client_no_proxies_succeeds() {
        let config = Config::default();
        let client = build_reqwest_client(&config).expect("build_reqwest_client with no proxies");
        drop(client);
    }

    #[test]
    fn build_client_with_valid_proxies_succeeds() {
        let mut config = Config::default();
        config.core.proxies = vec![
            "http://proxy1.example.com:8080".to_string(),
            "socks5://proxy2.example.com:1080".to_string(),
        ];
        // Build should succeed; actual connectivity is not tested in unit tests.
        let client = build_client(&config).expect("build_client with valid proxy URLs");
        drop(client);
    }
}
