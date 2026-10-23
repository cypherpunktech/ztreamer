//! Client behavior against an in-process HTTP server that plays the node.

use std::{
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::Bytes;
use http::{Request, Response, StatusCode, header};
use http_body_util::{BodyExt as _, Full};
use hyper::{body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::oneshot};
use ztreamer_remote::{Auth, CallOptions, RetryPolicy, RpcClient, RpcCode, RpcConfig, RpcError};

/// What the mock does with one request.
enum Reply {
    /// Answer `result` in a well-formed envelope with the request's id.
    Result(Value),
    /// Answer with a JSON-RPC error object.
    Error(i64, &'static str),
    /// Answer with this HTTP status and body.
    Status(u16, &'static str),
    /// Wait, then answer with the inner reply.
    Delay(Duration, Box<Reply>),
    /// Close the connection without a response, like zakura on bad credentials.
    Close,
}

/// One request as the mock saw it.
#[derive(Clone, Debug)]
struct Seen {
    authorization: Option<String>,
    content_type: Option<String>,
    body: Value,
}

type Script = Arc<dyn Fn(usize, &Seen) -> Reply + Send + Sync>;

struct Mock {
    addr: SocketAddr,
    seen: Arc<Mutex<Vec<Seen>>>,
    _stop: oneshot::Sender<()>,
}

impl Mock {
    async fn start(script: impl Fn(usize, &Seen) -> Reply + Send + Sync + 'static) -> Self {
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
                                            200,
                                            json!({"jsonrpc": "2.0", "result": result, "id": id}),
                                        ));
                                    }
                                    Reply::Error(code, message) => {
                                        return Ok(json_response(
                                            200,
                                            json!({"jsonrpc": "2.0", "error": {"code": code, "message": message}, "id": id}),
                                        ));
                                    }
                                    Reply::Status(status, body) => {
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

    fn config(&self) -> RpcConfig {
        RpcConfig::new(format!("http://{}", self.addr).parse().unwrap())
            .with_retry(RetryPolicy::NONE)
    }

    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }
}

fn json_response(status: u16, body: Value) -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::from_u16(status).unwrap())
        .header(header::CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(serde_json::to_vec(&body).unwrap())))
        .unwrap()
}

const fn fast_retries(max_retries: u32) -> RetryPolicy {
    RetryPolicy {
        max_retries,
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(5),
    }
}

#[derive(Debug, Deserialize, PartialEq)]
struct Tip {
    height: u32,
    hash: String,
}

#[tokio::test]
async fn round_trip_with_typed_result() {
    let mock = Mock::start(|_, _| Reply::Result(json!({"height": 5, "hash": "ab"}))).await;
    let client = RpcClient::new(mock.config()).unwrap();

    let tip: Tip = client.call("getblock", ("5", 1)).await.unwrap();
    assert_eq!(tip, Tip { height: 5, hash: "ab".into() });

    let seen = mock.seen();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].content_type.as_deref(), Some("application/json"));
    assert_eq!(seen[0].authorization, None);
    assert_eq!(seen[0].body["jsonrpc"], "2.0");
    assert_eq!(seen[0].body["method"], "getblock");
    assert_eq!(seen[0].body["params"], json!(["5", 1]));
    assert!(seen[0].body["id"].is_u64());

    // Ids advance so a stale answer can never be taken for a fresh one.
    let _: Tip = client.call("getblock", ("6", 1)).await.unwrap();
    let seen = mock.seen();
    assert_ne!(seen[0].body["id"], seen[1].body["id"]);
}

#[tokio::test]
async fn unit_params_serialize_as_empty_array() {
    let mock = Mock::start(|_, _| Reply::Result(json!("hash"))).await;
    let client = RpcClient::new(mock.config()).unwrap();
    let hash: String = client.call("getbestblockhash", ()).await.unwrap();
    assert_eq!(hash, "hash");
    assert_eq!(mock.seen()[0].body["params"], json!([]));
}

#[tokio::test]
async fn cookie_file_sets_basic_auth_header() {
    let dir = tempfile::tempdir().unwrap();
    let cookie = dir.path().join(".cookie");
    std::fs::write(&cookie, "__cookie__:s3cret\n").unwrap();

    let mock = Mock::start(|_, _| Reply::Result(json!(1))).await;
    let client = RpcClient::new(mock.config().with_auth(Auth::Cookie(cookie))).unwrap();
    let _: u32 = client.call("getblockcount", ()).await.unwrap();

    let expected = format!("Basic {}", STANDARD.encode(b"__cookie__:s3cret"));
    assert_eq!(mock.seen()[0].authorization.as_deref(), Some(expected.as_str()));
}

#[tokio::test]
async fn basic_auth_sets_header() {
    let mock = Mock::start(|_, _| Reply::Result(json!(1))).await;
    let client = RpcClient::new(
        mock.config().with_auth(Auth::Basic { user: "ztreamer".into(), password: "pw".into() }),
    )
    .unwrap();
    let _: u32 = client.call("getblockcount", ()).await.unwrap();

    let expected = format!("Basic {}", STANDARD.encode(b"ztreamer:pw"));
    assert_eq!(mock.seen()[0].authorization.as_deref(), Some(expected.as_str()));
}

#[tokio::test]
async fn node_error_object_is_not_retried() {
    let mock = Mock::start(|_, _| Reply::Error(-8, "Invalid parameter")).await;
    let client = RpcClient::new(mock.config().with_retry(fast_retries(3))).unwrap();

    let error = client.call::<_, Value>("getblock", ("x",)).await.unwrap_err();
    match &error {
        RpcError::Rpc { code, message, .. } => {
            assert_eq!(*code, RpcCode(-8));
            assert_eq!(message, "Invalid parameter");
        }
        other => panic!("unexpected {other:?}"),
    }
    assert_eq!(error.method(), Some("getblock"));
    assert_eq!(mock.seen().len(), 1);
}

#[tokio::test]
async fn restricted_listener_reports_method_not_found() {
    let mock = Mock::start(|_, _| Reply::Error(-32601, "Method not found")).await;
    let client = RpcClient::new(mock.config()).unwrap();
    let error = client.call::<_, Value>("z_gettreestate", ("1",)).await.unwrap_err();
    assert!(error.is_method_not_found());
}

#[tokio::test]
async fn method_timeout_overrides_default() {
    let mock = Mock::start(|_, _| {
        Reply::Delay(Duration::from_millis(400), Box::new(Reply::Result(json!(1))))
    })
    .await;
    let client = RpcClient::new(
        mock.config()
            .with_default_timeout(Duration::from_secs(5))
            .with_method_timeout("getblock", Duration::from_millis(50)),
    )
    .unwrap();

    let started = Instant::now();
    let error = client.call::<_, Value>("getblock", ("1",)).await.unwrap_err();
    assert!(started.elapsed() < Duration::from_millis(350), "{:?}", started.elapsed());
    match error {
        RpcError::Timeout { method, timeout } => {
            assert_eq!(method, "getblock");
            assert_eq!(timeout, Duration::from_millis(50));
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[tokio::test]
async fn call_options_override_timeout_and_retries() {
    let mock = Mock::start(|_, _| {
        Reply::Delay(Duration::from_millis(100), Box::new(Reply::Result(json!(1))))
    })
    .await;
    let client = RpcClient::new(
        mock.config().with_default_timeout(Duration::from_millis(20)).with_retry(fast_retries(5)),
    )
    .unwrap();

    // Per-call timeout long enough: succeeds on the first attempt.
    let value: u32 = client
        .call_with(
            "getblock",
            ("1",),
            CallOptions { timeout: Some(Duration::from_secs(2)), max_retries: None },
        )
        .await
        .unwrap();
    assert_eq!(value, 1);
    assert_eq!(mock.seen().len(), 1);

    // Default timeout too short and retries disabled per call: one attempt, one timeout.
    let error = client
        .call_with::<_, Value>("getblock", ("1",), CallOptions::no_retry())
        .await
        .unwrap_err();
    assert!(matches!(error, RpcError::Timeout { .. }));
    assert_eq!(mock.seen().len(), 2);
}

#[tokio::test]
async fn retries_503_then_succeeds() {
    let mock =
        Mock::start(
            |nth, _| {
                if nth < 2 { Reply::Status(503, "busy") } else { Reply::Result(json!(42)) }
            },
        )
        .await;
    let client = RpcClient::new(mock.config().with_retry(fast_retries(3))).unwrap();
    let value: u32 = client.call("getblockcount", ()).await.unwrap();
    assert_eq!(value, 42);
    assert_eq!(mock.seen().len(), 3);
}

#[tokio::test]
async fn retries_warmup_then_gives_up() {
    let mock = Mock::start(|_, _| Reply::Error(-28, "Loading block index")).await;
    let client = RpcClient::new(mock.config().with_retry(fast_retries(2))).unwrap();
    let error = client.call::<_, Value>("getblockcount", ()).await.unwrap_err();
    assert_eq!(error.rpc_code(), Some(RpcCode::IN_WARMUP));
    assert_eq!(mock.seen().len(), 3, "first attempt plus two retries");
}

#[tokio::test]
async fn http_500_is_reported_with_body_and_not_retried() {
    let mock = Mock::start(|_, _| Reply::Status(500, "boom")).await;
    let client = RpcClient::new(mock.config().with_retry(fast_retries(3))).unwrap();
    let error = client.call::<_, Value>("getblockcount", ()).await.unwrap_err();
    match error {
        RpcError::Http { status, body, .. } => {
            assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
            assert_eq!(body, "boom");
        }
        other => panic!("unexpected {other:?}"),
    }
    assert_eq!(mock.seen().len(), 1);
}

#[tokio::test]
async fn http_401_is_unauthorized() {
    let mock = Mock::start(|_, _| Reply::Status(401, "")).await;
    let client = RpcClient::new(mock.config().with_retry(fast_retries(3))).unwrap();
    let error = client.call::<_, Value>("getblockcount", ()).await.unwrap_err();
    assert!(matches!(error, RpcError::Unauthorized { status: StatusCode::UNAUTHORIZED, .. }));
    assert_eq!(mock.seen().len(), 1);
}

#[tokio::test]
async fn dropped_connection_names_the_cookie() {
    let mock = Mock::start(|_, _| Reply::Close).await;
    let client = RpcClient::new(mock.config()).unwrap();
    let error = client.call::<_, Value>("getblockcount", ()).await.unwrap_err();
    match &error {
        RpcError::ConnectionClosed { hint, .. } => assert!(hint.contains("cookie"), "{hint}"),
        other => panic!("unexpected {other:?}"),
    }
    assert!(error.is_retryable());
    assert_eq!(mock.seen().len(), 1);
}

#[tokio::test]
async fn cookie_is_reloaded_after_a_dropped_connection() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(".cookie");
    std::fs::write(&path, "__cookie__:old\n").unwrap();
    let accepted = format!("Basic {}", STANDARD.encode(b"__cookie__:new"));
    let reject_all = Arc::new(AtomicBool::new(false));

    let reject = reject_all.clone();
    let mock = Mock::start(move |_, seen| {
        if !reject.load(Ordering::SeqCst)
            && seen.authorization.as_deref() == Some(accepted.as_str())
        {
            Reply::Result(json!(7))
        } else {
            Reply::Close
        }
    })
    .await;
    let client = RpcClient::new(mock.config().with_auth(Auth::Cookie(path.clone()))).unwrap();

    // The node "restarted" and rotated its cookie after the client read it.
    std::fs::write(&path, "__cookie__:new\n").unwrap();

    let value: u32 = client.call("getblockcount", ()).await.unwrap();
    assert_eq!(value, 7);
    let seen = mock.seen();
    assert_eq!(seen.len(), 2, "one rejected attempt, one with the fresh cookie");
    assert_ne!(seen[0].authorization, seen[1].authorization);

    // A drop with an unchanged cookie file gets no second attempt.
    reject_all.store(true, Ordering::SeqCst);
    let error = client.call::<_, Value>("getblockcount", ()).await.unwrap_err();
    assert!(matches!(error, RpcError::ConnectionClosed { .. }));
    assert_eq!(mock.seen().len(), 3, "reload found nothing new, so no retry");
}

#[tokio::test]
async fn decode_failure_names_the_method() {
    let mock = Mock::start(|_, _| Reply::Result(json!("not a number"))).await;
    let client = RpcClient::new(mock.config()).unwrap();
    let error = client.call::<_, u32>("getblockcount", ()).await.unwrap_err();
    assert!(matches!(error, RpcError::Decode { .. }));
    assert_eq!(error.method(), Some("getblockcount"));
}

#[tokio::test]
async fn unreachable_node_is_a_transport_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let client = RpcClient::new(RpcConfig::new(format!("http://{addr}").parse().unwrap())).unwrap();
    let error = client.call::<_, Value>("getblockcount", ()).await.unwrap_err();
    assert!(matches!(error, RpcError::Transport { .. }), "{error:?}");
    assert!(error.is_retryable());
}
