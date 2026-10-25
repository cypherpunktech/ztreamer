//! A mock zakura node for tests. It is an HTTP server in the test process. A script sets the
//! reply to each request.
//!
//! The tests of this crate use it. Tests in crates that use this crate can also use it. This
//! module compiles for the tests of this crate and when the `test-utils` feature is on.

use std::{
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use http::{Request, Response, StatusCode, Uri, header};
use http_body_util::{BodyExt as _, Full};
use hyper::{body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::oneshot};

use crate::{RetryPolicy, RpcCode, RpcConfig};

/// What the mock does with one request.
#[derive(Debug)]
pub enum Reply {
    /// Answer `result` in a well-formed envelope with the request's id.
    Result(Value),
    /// Answer with a JSON-RPC error object.
    Error {
        /// The error code.
        code: RpcCode,
        /// The error message.
        message: &'static str,
    },
    /// Answer with this HTTP status and body.
    Status {
        /// The HTTP status.
        status: StatusCode,
        /// The response body.
        body: &'static str,
    },
    /// Wait, then answer with the inner reply.
    Delay(Duration, Box<Reply>),
    /// Close the connection without a response, like zakura on bad credentials.
    Close,
}

/// One request as the mock saw it.
#[derive(Clone, Debug)]
pub struct Seen {
    /// The `Authorization` header, if the request has one.
    pub authorization: Option<String>,
    /// The `Content-Type` header, if the request has one.
    pub content_type: Option<String>,
    /// The JSON-RPC request body.
    pub body: Value,
}

type Script = Arc<dyn Fn(usize, &Seen) -> Reply + Send + Sync>;

/// A mock node that runs. The server stops when you drop this value.
#[derive(Debug)]
pub struct MockNode {
    addr: SocketAddr,
    seen: Arc<Mutex<Vec<Seen>>>,
    _stop: oneshot::Sender<()>,
}

impl MockNode {
    /// Starts the server on a free local port. The reply to each request is
    /// `script(n, request)`. `n` is the request number on all connections, from zero.
    pub async fn start(script: impl Fn(usize, &Seen) -> Reply + Send + Sync + 'static) -> Self {
        let script: Script = Arc::new(script);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let counter = Arc::new(AtomicUsize::new(0));
        let (stop, mut stopped) = oneshot::channel::<()>();
        let seen_for_task = seen.clone();
        tokio::spawn(async move {
            loop {
                let stream = tokio::select! {
                    _ = &mut stopped => break,
                    accepted = listener.accept() => accepted.unwrap().0,
                };
                let script = script.clone();
                let seen = seen_for_task.clone();
                let counter = counter.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |request: Request<Incoming>| {
                        let script = script.clone();
                        let seen = seen.clone();
                        let counter = counter.clone();
                        async move {
                            let (parts, body) = request.into_parts();
                            let body = body.collect().await.unwrap().to_bytes();
                            let header = |name: header::HeaderName| {
                                parts
                                    .headers
                                    .get(name)
                                    .map(|value| value.to_str().unwrap().to_owned())
                            };
                            let request = Seen {
                                authorization: header(header::AUTHORIZATION),
                                content_type: header(header::CONTENT_TYPE),
                                body: serde_json::from_slice(&body).unwrap(),
                            };
                            let nth = counter.fetch_add(1, Ordering::SeqCst);
                            let mut reply = script(nth, &request);
                            let id = request.body["id"].clone();
                            seen.lock().unwrap().push(request);
                            loop {
                                reply = match reply {
                                    Reply::Delay(wait, inner) => {
                                        tokio::time::sleep(wait).await;
                                        *inner
                                    }
                                    Reply::Result(result) => {
                                        return Ok(json_response(
                                            StatusCode::OK,
                                            json!({"jsonrpc": "2.0", "result": result, "id": id}),
                                        ));
                                    }
                                    Reply::Error { code, message } => {
                                        return Ok(json_response(
                                            StatusCode::OK,
                                            json!({"jsonrpc": "2.0", "error": {"code": code, "message": message}, "id": id}),
                                        ));
                                    }
                                    Reply::Status { status, body } => {
                                        return Ok(Response::builder()
                                            .status(status)
                                            .body(Full::new(Bytes::from_static(body.as_bytes())))
                                            .unwrap());
                                    }
                                    Reply::Close => {
                                        return Err::<Response<Full<Bytes>>, _>(
                                            std::io::Error::other("scripted close"),
                                        );
                                    }
                                };
                            }
                        }
                    });
                    // An Err from the service makes hyper drop the connection unanswered.
                    let _ =
                        http1::Builder::new().serve_connection(TokioIo::new(stream), service).await;
                });
            }
        });
        Self { addr, seen, _stop: stop }
    }

    /// The address of the server.
    pub const fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// The URL of the server.
    pub fn url(&self) -> Uri {
        Uri::builder()
            .scheme("http")
            .authority(self.addr.to_string())
            .path_and_query("/")
            .build()
            .expect("a socket address is a valid authority")
    }

    /// A client configuration for this server. Retries are off.
    pub fn config(&self) -> RpcConfig {
        RpcConfig::new(self.url()).with_retry(RetryPolicy::NONE)
    }

    /// All requests that the server received, in the order of arrival.
    pub fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }
}

fn json_response(status: StatusCode, body: Value) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(serde_json::to_vec(&body).unwrap())))
        .unwrap()
}
