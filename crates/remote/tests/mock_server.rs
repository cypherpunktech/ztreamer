//! Client behavior against an in-process HTTP server that plays the node.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use http::StatusCode;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use ztreamer_remote::{
    Auth, RetryPolicy, RpcClient, RpcCode, RpcConfig, RpcError, Secret, ZakuraClient,
    test_utils::{MockNode, Reply},
};

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
    let mock = MockNode::start(|_, _| Reply::Result(json!({"height": 5, "hash": "ab"}))).await;
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
    let mock = MockNode::start(|_, _| Reply::Result(json!("hash"))).await;
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

    let mock = MockNode::start(|_, _| Reply::Result(json!(1))).await;
    let client = RpcClient::new(mock.config().with_auth(Auth::Cookie(cookie))).unwrap();
    let _: u32 = client.call("getblockcount", ()).await.unwrap();

    let expected = format!("Basic {}", STANDARD.encode(b"__cookie__:s3cret"));
    assert_eq!(mock.seen()[0].authorization.as_deref(), Some(expected.as_str()));
}

#[tokio::test]
async fn basic_auth_sets_header() {
    let mock = MockNode::start(|_, _| Reply::Result(json!(1))).await;
    let client = RpcClient::new(
        mock.config()
            .with_auth(Auth::Basic { user: "ztreamer".into(), password: Secret::new("pw") }),
    )
    .unwrap();
    let _: u32 = client.call("getblockcount", ()).await.unwrap();

    let expected = format!("Basic {}", STANDARD.encode(b"ztreamer:pw"));
    assert_eq!(mock.seen()[0].authorization.as_deref(), Some(expected.as_str()));
}

#[tokio::test]
async fn node_error_object_is_not_retried() {
    let mock =
        MockNode::start(|_, _| Reply::Error { code: RpcCode(-8), message: "Invalid parameter" })
            .await;
    let client = RpcClient::new(mock.config().with_retry(fast_retries(3))).unwrap();

    let error = client.call::<_, Value>("getblock", ("x",)).await.unwrap_err();
    match &error {
        RpcError::Rpc { method, code, message } => {
            assert_eq!(method, "getblock");
            assert_eq!(*code, RpcCode(-8));
            assert_eq!(message, "Invalid parameter");
        }
        other => panic!("unexpected {other:?}"),
    }
    assert_eq!(mock.seen().len(), 1);
}

#[tokio::test]
async fn restricted_listener_reports_method_not_found() {
    let mock = MockNode::start(|_, _| Reply::Error {
        code: RpcCode::METHOD_NOT_FOUND,
        message: "Method not found",
    })
    .await;
    let client = RpcClient::new(mock.config()).unwrap();
    let error = client.call::<_, Value>("z_gettreestate", ("1",)).await.unwrap_err();
    assert!(error.is_method_not_found());
}

#[tokio::test]
async fn method_timeout_overrides_default() {
    let mock = MockNode::start(|_, _| {
        Reply::Delay(Duration::from_millis(400), Box::new(Reply::Result(json!(1))))
    })
    .await;
    let client =
        RpcClient::new(mock.config().with_method_timeout("getblock", Duration::from_millis(50)))
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
async fn retries_503_then_succeeds() {
    let mock = MockNode::start(|nth, _| {
        if nth < 2 {
            Reply::Status { status: StatusCode::SERVICE_UNAVAILABLE, body: "busy" }
        } else {
            Reply::Result(json!(42))
        }
    })
    .await;
    let client = RpcClient::new(mock.config().with_retry(fast_retries(3))).unwrap();
    let value: u32 = client.call("getblockcount", ()).await.unwrap();
    assert_eq!(value, 42);
    assert_eq!(mock.seen().len(), 3);
}

#[tokio::test]
async fn retries_warmup_then_gives_up() {
    let mock = MockNode::start(|_, _| Reply::Error {
        code: RpcCode::IN_WARMUP,
        message: "Loading block index",
    })
    .await;
    let client = RpcClient::new(mock.config().with_retry(fast_retries(2))).unwrap();
    let error = client.call::<_, Value>("getblockcount", ()).await.unwrap_err();
    assert_eq!(error.rpc_code(), Some(RpcCode::IN_WARMUP));
    assert_eq!(mock.seen().len(), 3, "first attempt plus two retries");
}

#[tokio::test]
async fn http_500_is_reported_with_body_and_not_retried() {
    let mock = MockNode::start(|_, _| Reply::Status {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        body: "boom",
    })
    .await;
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
    let mock =
        MockNode::start(|_, _| Reply::Status { status: StatusCode::UNAUTHORIZED, body: "" }).await;
    let client = RpcClient::new(mock.config().with_retry(fast_retries(3))).unwrap();
    let error = client.call::<_, Value>("getblockcount", ()).await.unwrap_err();
    assert!(matches!(error, RpcError::Unauthorized { status: StatusCode::UNAUTHORIZED, .. }));
    assert_eq!(mock.seen().len(), 1);
}

#[tokio::test]
async fn dropped_connection_names_the_cookie() {
    let mock = MockNode::start(|_, _| Reply::Close).await;
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
    let mock = MockNode::start(move |_, seen| {
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
    let mock = MockNode::start(|_, _| Reply::Result(json!("not a number"))).await;
    let client = RpcClient::new(mock.config()).unwrap();
    let error = client.call::<_, u32>("getblockcount", ()).await.unwrap_err();
    assert!(matches!(&error, RpcError::Decode { method, .. } if method == "getblockcount"));
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

#[tokio::test]
async fn send_raw_transaction_is_not_retried_after_the_node_answers() {
    let mock = MockNode::start(|_, _| Reply::Status {
        status: StatusCode::SERVICE_UNAVAILABLE,
        body: "busy",
    })
    .await;
    let node =
        ZakuraClient::new(RpcClient::new(mock.config().with_retry(fast_retries(3))).unwrap());
    let error = node.send_raw_transaction(&[0xde, 0xad]).await.unwrap_err();
    assert!(error.is_retryable(), "{error}");
    assert_eq!(mock.seen().len(), 1);
}

#[tokio::test]
async fn a_refused_connection_is_unsent() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let client = RpcClient::new(RpcConfig::new(format!("http://{addr}").parse().unwrap())).unwrap();
    let error = client.call::<_, Value>("getblockcount", ()).await.unwrap_err();
    assert!(error.is_unsent(), "{error:?}");
}

#[tokio::test]
async fn send_raw_transaction_retries_a_refused_connection() {
    // The port refuses connections until the server starts on it, after the first attempt.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let server = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        let listener = TcpListener::bind(addr).await.unwrap();
        let (stream, _) = listener.accept().await.unwrap();
        let service = hyper::service::service_fn(
            |request: hyper::Request<hyper::body::Incoming>| async move {
                let body =
                    http_body_util::BodyExt::collect(request.into_body()).await.unwrap().to_bytes();
                let request: Value = serde_json::from_slice(&body).unwrap();
                let reply =
                    json!({"jsonrpc": "2.0", "result": "00".repeat(32), "id": request["id"]});
                Ok::<_, std::convert::Infallible>(hyper::Response::new(http_body_util::Full::new(
                    bytes::Bytes::from(serde_json::to_vec(&reply).unwrap()),
                )))
            },
        );
        hyper::server::conn::http1::Builder::new()
            .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
            .await
            .unwrap();
    });
    let retries = RetryPolicy {
        max_retries: 20,
        initial_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_millis(10),
    };
    let config = RpcConfig::new(format!("http://{addr}").parse().unwrap()).with_retry(retries);
    let node = ZakuraClient::new(RpcClient::new(config).unwrap());
    let sent = node.send_raw_transaction(&[0xde, 0xad]).await.unwrap();
    assert_eq!(sent.hash().to_string(), "00".repeat(32));
    server.abort();
}
