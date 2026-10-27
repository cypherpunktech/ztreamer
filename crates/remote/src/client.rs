//! The client: connection pool, auth header, timeouts, retries.

use std::{
    collections::HashMap,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use http::{
    HeaderValue, StatusCode, Uri,
    header::{AUTHORIZATION, CONTENT_TYPE},
};
use http_body_util::{BodyExt as _, Full};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::{Auth, HttpsClient, RpcError, envelope};

/// How many bytes of an unexpected HTTP body end up in the error message.
const ERROR_BODY_PREVIEW: usize = 512;

/// The TCP connect timeout.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Backoff and attempt limits for retryable failures.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Attempts after the first one. Zero disables retries.
    pub max_retries: u32,
    /// Wait before the first retry. Doubles on each further retry.
    pub initial_backoff: Duration,
    /// Cap on the wait between retries.
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff: Duration::from_millis(200),
            max_backoff: Duration::from_secs(2),
        }
    }
}

impl RetryPolicy {
    /// No retries at all.
    #[cfg(any(test, feature = "test-utils"))]
    pub const NONE: Self =
        Self { max_retries: 0, initial_backoff: Duration::ZERO, max_backoff: Duration::ZERO };

    fn backoff(&self, retry: u32) -> Duration {
        let factor = 2u32.saturating_pow(retry);
        self.initial_backoff.saturating_mul(factor).min(self.max_backoff)
    }
}

/// Everything needed to reach one node. Build it with [`RpcConfig::new`] and the `with_*`
/// methods.
#[derive(Clone, Debug)]
pub struct RpcConfig {
    /// `http://host:port` or `https://host:port`. The client checks the scheme and the host.
    url: Uri,
    /// Credentials.
    auth: Auth,
    /// Timeout for a method without an entry in `method_timeouts`.
    default_timeout: Duration,
    /// Per-method timeouts. Slow methods such as `z_gettreestate` go here.
    method_timeouts: HashMap<String, Duration>,
    /// Retry policy for failures that are safe to retry.
    retry: RetryPolicy,
}

impl RpcConfig {
    /// A configuration with no auth and the default timeouts and retries.
    pub fn new(url: Uri) -> Self {
        Self {
            url,
            auth: Auth::None,
            default_timeout: Duration::from_secs(15),
            method_timeouts: HashMap::new(),
            retry: RetryPolicy::default(),
        }
    }

    /// The node URL.
    pub const fn url(&self) -> &Uri {
        &self.url
    }

    /// Sets the credentials.
    pub fn with_auth(mut self, auth: Auth) -> Self {
        self.auth = auth;
        self
    }

    /// Sets the timeout for one method.
    pub fn with_method_timeout(mut self, method: impl Into<String>, timeout: Duration) -> Self {
        self.method_timeouts.insert(method.into(), timeout);
        self
    }

    /// Sets the retry policy.
    #[cfg(any(test, feature = "test-utils"))]
    pub const fn with_retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }
}

/// The failures that a call retries, up to the retry count of the [`RetryPolicy`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Retries {
    /// Retries the failures that [`RpcError::is_retryable`] accepts.
    Retryable,
    /// Retries only a failure where the request did not reach the node
    /// ([`RpcError::is_unsent`]). Use this for a call that must not run twice on the node.
    Unsent,
}

type HttpClient = HttpsClient<Full<Bytes>>;

struct Inner {
    http: HttpClient,
    uri: Uri,
    auth: Auth,
    auth_header: RwLock<Option<HeaderValue>>,
    default_timeout: Duration,
    method_timeouts: HashMap<String, Duration>,
    retry: RetryPolicy,
    next_id: AtomicU64,
}

/// A cheap-to-clone handle to one node's JSON-RPC listener.
#[derive(Clone)]
pub struct RpcClient {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for RpcClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcClient")
            .field("url", &self.inner.uri)
            .field("auth", &self.inner.auth)
            .finish_non_exhaustive()
    }
}

impl RpcClient {
    /// Builds the connection pool and reads the credentials once.
    ///
    /// Fails on a bad URL, an unreadable cookie, or a TLS setup error. It does not
    /// contact the node.
    pub fn new(config: RpcConfig) -> Result<Self, RpcError> {
        let uri = config.url;
        match uri.scheme_str() {
            Some("http") | Some("https") => {}
            other => {
                return Err(RpcError::Config(format!(
                    "url {uri} must use http or https, got {other:?}"
                )));
            }
        }
        if uri.authority().is_none() {
            return Err(RpcError::Config(format!("url {uri} has no host")));
        }

        let auth_header = config.auth.header()?;
        let http = HttpClient::new(Some(CONNECT_TIMEOUT))
            .map_err(|error| RpcError::Config(format!("tls setup failed: {error}")))?;

        Ok(Self {
            inner: Arc::new(Inner {
                http,
                uri,
                auth: config.auth,
                auth_header: RwLock::new(auth_header),
                default_timeout: config.default_timeout,
                method_timeouts: config.method_timeouts,
                retry: config.retry,
                next_id: AtomicU64::new(1),
            }),
        })
    }

    /// The node URL.
    pub fn url(&self) -> &Uri {
        &self.inner.uri
    }

    /// The timeout of a call to `method`.
    pub fn timeout_for(&self, method: &str) -> Duration {
        self.inner.method_timeouts.get(method).copied().unwrap_or(self.inner.default_timeout)
    }

    /// Calls `method` with `params` and decodes the result.
    ///
    /// `params` is any serializable value; use `()` for no parameters, a tuple or array for
    /// positional ones.
    pub async fn call<P, R>(&self, method: &str, params: P) -> Result<R, RpcError>
    where
        P: Serialize,
        R: DeserializeOwned,
    {
        self.call_with(method, params, Retries::Retryable).await
    }

    /// Calls `method` as [`Self::call`] does, but retries only the failures in `retries`.
    pub(crate) async fn call_with<P, R>(
        &self,
        method: &str,
        params: P,
        retries: Retries,
    ) -> Result<R, RpcError>
    where
        P: Serialize,
        R: DeserializeOwned,
    {
        let params = match serde_json::to_value(params) {
            // `()` serializes as null; zcash clients send an empty positional list instead.
            Ok(Value::Null) => Value::Array(Vec::new()),
            Ok(params) => params,
            Err(error) => {
                return Err(RpcError::Protocol {
                    method: method.to_owned(),
                    detail: format!("cannot serialize params: {error}"),
                });
            }
        };
        let value = self.call_value(method, params, retries).await?;
        serde_json::from_value(value)
            .map_err(|source| RpcError::Decode { method: method.to_owned(), source })
    }

    /// Calls `method` and returns the raw `result` value.
    async fn call_value(
        &self,
        method: &str,
        params: Value,
        retries: Retries,
    ) -> Result<Value, RpcError> {
        let timeout = self.timeout_for(method);
        let max_retries = self.inner.retry.max_retries;

        let mut retry_count = 0;
        let mut reloaded_cookie = false;
        loop {
            let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
            let body = serde_json::to_vec(&envelope::Request::new(id, method, params.clone()))
                .map_err(|error| RpcError::Protocol {
                    method: method.to_owned(),
                    detail: format!("cannot serialize request: {error}"),
                })?;

            let error = match self.once(method, id, Bytes::from(body), timeout).await {
                Ok(value) => return Ok(value),
                Err(error) => error,
            };

            // A rejected or dropped request with cookie auth usually means the node restarted
            // and rotated its cookie. Read the file again before giving up.
            if !reloaded_cookie
                && self.inner.auth.is_cookie()
                && matches!(
                    error,
                    RpcError::Unauthorized { .. } | RpcError::ConnectionClosed { .. }
                )
            {
                reloaded_cookie = true;
                if self.reload_cookie() {
                    tracing::info!(method, "remote rpc: cookie changed on disk, retrying");
                    continue;
                }
            }

            let retry = match retries {
                Retries::Retryable => error.is_retryable(),
                Retries::Unsent => error.is_unsent(),
            };
            if retry && retry_count < max_retries {
                let wait = self.inner.retry.backoff(retry_count);
                retry_count += 1;
                tracing::debug!(
                    method,
                    retry = retry_count,
                    max_retries,
                    ?wait,
                    %error,
                    "remote rpc: retrying"
                );
                tokio::time::sleep(wait).await;
                continue;
            }

            return Err(error);
        }
    }

    /// Re-reads the cookie file. Returns true when the header changed.
    fn reload_cookie(&self) -> bool {
        let Ok(fresh) = self.inner.auth.header() else {
            return false;
        };
        let mut current =
            self.inner.auth_header.write().unwrap_or_else(|poisoned| poisoned.into_inner());
        if *current == fresh {
            return false;
        }
        *current = fresh;
        true
    }

    async fn once(
        &self,
        method: &str,
        id: u64,
        body: Bytes,
        timeout: Duration,
    ) -> Result<Value, RpcError> {
        let mut request = http::Request::post(self.inner.uri.clone())
            .header(CONTENT_TYPE, "application/json")
            .body(Full::new(body))
            .map_err(|error| RpcError::Protocol {
                method: method.to_owned(),
                detail: format!("cannot build request: {error}"),
            })?;
        let auth =
            self.inner.auth_header.read().unwrap_or_else(|poisoned| poisoned.into_inner()).clone();
        if let Some(auth) = auth {
            request.headers_mut().insert(AUTHORIZATION, auth);
        }

        let exchange = async {
            let response = self
                .inner
                .http
                .request(request)
                .await
                .map_err(|error| classify_client_error(method, error))?;
            let status = response.status();
            let body = response
                .into_body()
                .collect()
                .await
                .map_err(|error| RpcError::Transport {
                    method: method.to_owned(),
                    source: Box::new(error),
                })?
                .to_bytes();
            Ok::<_, RpcError>((status, body))
        };
        let (status, body) = tokio::time::timeout(timeout, exchange)
            .await
            .map_err(|_| RpcError::Timeout { method: method.to_owned(), timeout })??;

        match status {
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                Err(RpcError::Unauthorized { method: method.to_owned(), status })
            }
            status if status.is_success() => envelope::parse_result(method, id, &body),
            status => {
                Err(RpcError::Http { method: method.to_owned(), status, body: preview(&body) })
            }
        }
    }
}

fn classify_client_error(method: &str, error: hyper_util::client::legacy::Error) -> RpcError {
    let incomplete = std::error::Error::source(&error)
        .and_then(|source| source.downcast_ref::<hyper::Error>())
        .is_some_and(hyper::Error::is_incomplete_message);
    if incomplete {
        return RpcError::ConnectionClosed {
            method: method.to_owned(),
            hint: "zakura drops the connection when cookie auth is on and the credentials are \
                   wrong; check the cookie file or password",
        };
    }
    RpcError::Transport { method: method.to_owned(), source: Box::new(error) }
}

fn preview(body: &[u8]) -> String {
    let text = String::from_utf8_lossy(body);
    let mut end = text.len().min(ERROR_BODY_PREVIEW);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(url: &'static str) -> RpcConfig {
        RpcConfig::new(Uri::from_static(url))
    }

    #[test]
    fn config_validation() {
        assert!(matches!(RpcClient::new(config("ftp://x")), Err(RpcError::Config(_))));
        assert!(matches!(RpcClient::new(config("/no-host")), Err(RpcError::Config(_))));
        assert!(RpcClient::new(config("https://node.example:8232")).is_ok());
        let client = RpcClient::new(config("http://127.0.0.1:8232/")).unwrap();
        assert_eq!(client.url(), &Uri::from_static("http://127.0.0.1:8232/"));
    }

    #[test]
    fn per_method_timeouts() {
        let client = RpcClient::new(
            config("http://127.0.0.1:1")
                .with_method_timeout("z_gettreestate", Duration::from_secs(60)),
        )
        .unwrap();
        assert_eq!(client.timeout_for("getblock"), Duration::from_secs(15));
        assert_eq!(client.timeout_for("z_gettreestate"), Duration::from_secs(60));
    }

    #[test]
    fn backoff_doubles_and_caps() {
        let policy = RetryPolicy {
            max_retries: 5,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_millis(350),
        };
        assert_eq!(policy.backoff(0), Duration::from_millis(100));
        assert_eq!(policy.backoff(1), Duration::from_millis(200));
        assert_eq!(policy.backoff(2), Duration::from_millis(350));
        assert_eq!(policy.backoff(40), Duration::from_millis(350));
        assert_eq!(RetryPolicy::NONE.backoff(0), Duration::ZERO);
    }

    #[test]
    fn preview_cuts_on_char_boundary() {
        let body = "é".repeat(ERROR_BODY_PREVIEW);
        let cut = preview(body.as_bytes());
        assert!(cut.len() <= ERROR_BODY_PREVIEW);
        assert!(body.starts_with(&cut));
    }
}
