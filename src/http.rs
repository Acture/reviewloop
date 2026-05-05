//! Centralized factory for outbound HTTP clients.
//!
//! All `reqwest::Client` construction in the worker / backend / oauth / email
//! modules goes through [`build_client`] or [`build_reqwest_client`] so proxy
//! configuration applies uniformly.
//!
//! # Proxy pool strategy
//!
//! [`reqwest-proxy-pool`](https://crates.io/crates/reqwest-proxy-pool) 0.4 only
//! supports proxy lists fetched from remote URLs (`.sources(vec![url, ...])`)
//! and only SOCKS5/SOCKS5H — neither matches our use case (static
//! user-supplied list of HTTP / SOCKS proxies).  Therefore this module
//! implements a lightweight custom [`reqwest_middleware::Middleware`] that:
//!
//! - **Round-robins** across pre-built `reqwest::Client` instances (one per
//!   proxy URL) using an atomic counter so concurrent requests spread across
//!   the pool.
//! - **Sequentially fails over** on transient connection errors — when a
//!   proxy times out, refuses the connection, or fails the TLS handshake,
//!   the same request is re-attempted against the next proxy in the
//!   rotation.  HTTP-level errors (any 4xx / 5xx that completes the
//!   round-trip and produces a Response) are NOT retried — the upstream
//!   service answered, so the proxy is healthy.
//!
//! Health-check probing / cooldown for known-bad proxies is deferred to a
//! future phase.

use crate::config::Config;
use crate::db::Db;
use anyhow::{Context, Result};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tracing::warn;

/// Round-robin + per-request failover proxy middleware.
///
/// Pre-builds one `reqwest::Client` per configured proxy URL and cycles
/// through them with an atomic counter.  When [`handle`] is called it picks
/// a starting index, then iterates the proxy list in order, returning on the
/// first success.  Transient connection errors (see
/// [`is_transient_proxy_error`]) trigger failover to the next proxy; HTTP
/// errors that complete a round-trip do not.
///
/// Bodies that cannot be cloned (streamed bodies) fall back to a
/// single-attempt path against the first selected proxy — documented
/// limitation that does not affect the current PDF-upload path (which
/// reads the whole file into memory before constructing the multipart
/// request body).
struct RoundRobinProxyMiddleware {
    /// One client per proxy URL, built at construction time.
    clients: Vec<reqwest::Client>,
    /// Monotonically increasing counter; modulo `clients.len()` gives index.
    counter: Arc<AtomicUsize>,
    /// DB path + project_id for emitting `proxy_failover` events.
    /// `None` when no project context is available (warn-only preserved).
    event_target: Option<(std::path::PathBuf, String)>,
}

#[async_trait::async_trait]
impl reqwest_middleware::Middleware for RoundRobinProxyMiddleware {
    async fn handle(
        &self,
        req: reqwest::Request,
        _extensions: &mut http::Extensions,
        _next: reqwest_middleware::Next<'_>,
    ) -> reqwest_middleware::Result<reqwest::Response> {
        let n = self.clients.len();
        // `clients` is never empty when this middleware is installed.
        debug_assert!(n > 0, "proxy middleware installed with no clients");

        let start = self.counter.fetch_add(1, Ordering::Relaxed) % n;

        // Streamed bodies (e.g. tokio File) can't be re-tried because
        // try_clone returns None. Fall back to single-attempt, no failover.
        // The current PDF upload path constructs the body from a Vec<u8>
        // (whole file pre-loaded), so try_clone returns Some -- failover
        // works for that flow.
        if req.try_clone().is_none() {
            return self.clients[start]
                .execute(req)
                .await
                .map_err(reqwest_middleware::Error::Reqwest);
        }

        let mut last_err: Option<reqwest::Error> = None;
        for attempt in 0..n {
            let idx = (start + attempt) % n;
            // Safe: we already verified try_clone returns Some above, and
            // try_clone is idempotent for cloneable bodies.
            let req_clone = req
                .try_clone()
                .expect("request body became non-cloneable mid-loop (impossible)");

            match self.clients[idx].execute(req_clone).await {
                Ok(resp) => {
                    if attempt > 0 {
                        warn!(
                            proxy_index = idx,
                            attempts = attempt + 1,
                            "request succeeded after proxy failover"
                        );
                        self.emit_failover_event(idx, attempt, None);
                    }
                    return Ok(resp);
                }
                Err(e) if is_transient_proxy_error(&e) => {
                    warn!(
                        proxy_index = idx,
                        error = %e,
                        attempt = attempt + 1,
                        total_proxies = n,
                        "proxy attempt failed; trying next proxy"
                    );
                    last_err = Some(e);
                    continue;
                }
                Err(e) => {
                    // Non-transient: don't waste time on more proxies.
                    // Examples: body-serialization failures, redirect-loop
                    // errors -- these would fail identically against any
                    // proxy.
                    return Err(reqwest_middleware::Error::Reqwest(e));
                }
            }
        }

        warn!(
            total_proxies = n,
            "all proxies failed for request; returning last transient error"
        );
        let last = last_err
            .expect("loop ran at least once and last_err is set on every transient failure");
        self.emit_failover_event(n - 1, n, Some(&last.to_string()));
        Err(reqwest_middleware::Error::Reqwest(last))
    }
}

impl RoundRobinProxyMiddleware {
    /// Write a `proxy_failover` event to the DB when a proxy failover occurs.
    /// Opens a fresh `Db` connection from the stored path; dropped immediately.
    /// Skipped silently when no event target is configured.
    ///
    /// `job_id` and `paper_id` are not included in the payload because the
    /// middleware executes below the per-job call sites (in `backend.submit` /
    /// `backend.poll`).  Plumbing them through `reqwest_middleware::Extensions`
    /// would require each backend to stamp every `reqwest::Request` before
    /// dispatch — worthwhile but deferred to a future iteration (R4 option a).
    fn emit_failover_event(&self, failed_proxy_index: usize, attempt: usize, error: Option<&str>) {
        let Some((ref db_path, ref pid)) = self.event_target else {
            return;
        };
        let db = Db::new_file(db_path.clone());
        let payload = serde_json::json!({
            "failed_proxy_index": failed_proxy_index,
            "attempt": attempt,
            "error": error.unwrap_or("failover — subsequent proxy succeeded"),
        });
        if let Err(e) = db.add_event(Some(pid), None, "proxy_failover", payload) {
            warn!(error = %e, "failed to write proxy_failover event to db");
        }
    }
}

/// Heuristic for "this looks like the proxy itself misbehaved, retry on a
/// different one" vs "this is a real error that won't change with a
/// different proxy". Conservative: only retry on errors that have no HTTP
/// status (i.e. the request never completed) — connect refused, timeout,
/// TLS handshake, DNS, etc.
fn is_transient_proxy_error(err: &reqwest::Error) -> bool {
    err.is_connect() || err.is_timeout() || err.status().is_none()
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
///
/// Pass `db` and `project_id` to enable `proxy_failover` event recording.
/// When either is `None`, failovers are warn-logged only (legacy behaviour).
pub fn build_client(
    config: &Config,
    db: Option<&Db>,
    project_id: Option<&str>,
) -> Result<ClientWithMiddleware> {
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

    // Store db_path + project_id for event recording; PathBuf + String are Send+Sync.
    let event_target: Option<(std::path::PathBuf, String)> = db
        .zip(project_id)
        .map(|(d, pid)| (d.path.clone(), pid.to_owned()));

    let middleware = RoundRobinProxyMiddleware {
        clients: proxy_clients,
        counter: Arc::new(AtomicUsize::new(0)),
        event_target,
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
        let client = build_client(&config, None, None).expect("build_client with no proxies");
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
        let client = build_client(&config, None, None).expect("build_client with valid proxy URLs");
        drop(client);
    }

    #[test]
    fn is_transient_proxy_error_classifies_correctly() {
        // Build a request to an unroutable address to force a connect error.
        // 198.51.100.0/24 is the TEST-NET-2 reserved block; nothing routes there.
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(500))
            .build()
            .unwrap();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let err = rt.block_on(async {
            client
                .get("http://198.51.100.1:1")
                .send()
                .await
                .expect_err("connection to TEST-NET-2 must fail")
        });
        assert!(
            is_transient_proxy_error(&err),
            "connect error to unroutable host must be classified as transient: {err}"
        );
    }

    #[tokio::test]
    async fn round_robin_failover_skips_dead_proxies() {
        use axum::{Router, response::IntoResponse, routing::get};
        use std::net::SocketAddr;
        use tokio::net::TcpListener;

        // Spin up a working "proxy" -- actually a plain HTTP server. The
        // failover logic we want to exercise lives in the middleware, not
        // in the proxy protocol itself; treating these endpoints as direct
        // upstreams via reqwest::Proxy::all is enough to verify that a dead
        // proxy URL gets skipped and a live one returns the response.
        async fn ok_handler() -> impl IntoResponse {
            "ok-from-live-proxy"
        }
        let app = Router::new().route("/", get(ok_handler));
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let live_addr = listener.local_addr().unwrap();
        let server_handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        // Two dead proxies (TEST-NET-2, unroutable) plus one live one at the end.
        // start counter is 0 on first request, so we'll attempt index 0 (dead),
        // then 1 (dead), then 2 (live) -- exercising the full failover loop.
        let mut config = Config::default();
        config.core.proxies = vec![
            "http://198.51.100.1:1".to_string(),
            "http://198.51.100.2:2".to_string(),
            format!("http://{live_addr}"),
        ];
        let client = build_client(&config, None, None).expect("build client with mixed proxies");

        // Request at the live "proxy" itself so the first two genuinely fail
        // at the connect step rather than getting an HTTP error from a
        // working proxy.
        let resp = client
            .get(format!("http://{live_addr}/"))
            .send()
            .await
            .expect("request must succeed via 3rd proxy after 2 failover steps");
        let status = resp.status();
        let body = resp.text().await.unwrap();
        assert!(status.is_success(), "got {status}: {body}");
        assert_eq!(body, "ok-from-live-proxy");

        server_handle.abort();
    }

    #[tokio::test]
    async fn failover_writes_proxy_failover_event_to_db() {
        use axum::{Router, response::IntoResponse, routing::get};
        use std::net::SocketAddr;
        use tempfile::tempdir;
        use tokio::net::TcpListener;

        async fn ok_handler() -> impl IntoResponse {
            "ok"
        }
        let app = Router::new().route("/", get(ok_handler));
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let live_addr = listener.local_addr().unwrap();
        let server_handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let tmp = tempdir().unwrap();
        let db = crate::db::Db::new(tmp.path());
        db.init_schema().unwrap();

        let mut config = Config::default();
        // One dead proxy, one live.
        config.core.proxies = vec![
            "http://198.51.100.1:1".to_string(),
            format!("http://{live_addr}"),
        ];
        let project_id = "test-proj-failover";
        let client =
            build_client(&config, Some(&db), Some(project_id)).expect("build client for test");

        let resp = client
            .get(format!("http://{live_addr}/"))
            .send()
            .await
            .expect("request must succeed via 2nd proxy");
        assert!(resp.status().is_success());

        // Give a moment for the event write (synchronous but confirm it happened).
        let failover_event = db
            .most_recent_event_of_type(project_id, "proxy_failover")
            .unwrap();
        assert!(
            failover_event.is_some(),
            "expected a proxy_failover event in db after failover"
        );

        server_handle.abort();
    }
}
