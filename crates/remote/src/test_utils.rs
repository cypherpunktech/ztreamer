//! A mock zakura node for tests. It is an HTTP server in the test process. A script sets the
//! reply to each request. [`MockChain`] is a script that serves a best chain of blocks.
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
use zakura_rpc::client::{
    GetBlockHashResponse, HexData,
    zakura_chain::{
        block::{self, Block},
        serialization::ZcashSerialize as _,
    },
};

use zakura_state::HashOrHeight;

use crate::{Method, RetryPolicy, RpcCode, RpcConfig};

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

/// A best chain that a [`MockNode`] serves, with the answers and error codes of zakura.
///
/// [`Self::reply`] answers [`Method::GetBlockchainInfo`], [`Method::GetBestBlockHash`],
/// [`Method::GetBlockHash`] and [`Method::GetBlock`] with verbosity 0. Params that do not decode
/// get "invalid params". Other requests get "method not found". Clones share the chain, thus a
/// test can change the chain while the node runs.
///
/// ```no_run
/// use ztreamer_remote::test_utils::{MockChain, MockNode};
/// # use zakura_rpc::client::zakura_chain::block::Block;
///
/// # async fn demo(blocks: Vec<Block>) {
/// let chain = MockChain::new(&blocks);
/// let node = MockNode::start({
///     let chain = chain.clone();
///     move |_, request| chain.reply(request)
/// })
/// .await;
/// # let _ = node;
/// # }
/// ```
#[derive(Clone, Debug, Default)]
pub struct MockChain {
    state: Arc<Mutex<ChainState>>,
}

#[derive(Debug, Default)]
struct ChainState {
    /// The hash and the serialized bytes of the block at each height, from genesis.
    blocks: Vec<(block::Hash, Vec<u8>)>,
    prune_height: Option<block::Height>,
}

impl MockChain {
    /// A chain of `blocks`, in which the block at index `n` has height `n`.
    pub fn new(blocks: &[Block]) -> Self {
        let chain = Self::default();
        chain.set_blocks(blocks);
        chain
    }

    /// Replaces the best chain with `blocks`, as a reorg or new blocks do.
    pub fn set_blocks(&self, blocks: &[Block]) {
        self.state.lock().unwrap().blocks = blocks
            .iter()
            .map(|block| (block.hash(), block.zcash_serialize_to_vec().unwrap()))
            .collect();
    }

    /// Sets the prune height that `getblockchaininfo` reports. `None` reports a node that does
    /// not prune.
    pub fn set_prune_height(&self, height: Option<block::Height>) {
        self.state.lock().unwrap().prune_height = height;
    }

    /// The reply of zakura with this chain to `request`.
    pub fn reply(&self, request: &Seen) -> Reply {
        let state = self.state.lock().unwrap();
        let params = request.body["params"].clone();
        let Some(method) = request.body["method"].as_str().and_then(Method::from_name) else {
            return METHOD_NOT_FOUND;
        };
        match method {
            Method::GetBlockchainInfo => state.blockchain_info(),
            Method::GetBestBlockHash => {
                state.blocks.last().map_or(NO_TIP, |(hash, _)| hash_reply(*hash))
            }
            Method::GetBlockHash => match serde_json::from_value::<(u32,)>(params) {
                Ok((height,)) => state.at(height).map_or(ABOVE_TIP, |(hash, _)| hash_reply(*hash)),
                Err(_) => INVALID_PARAMS,
            },
            Method::GetBlock => match serde_json::from_value::<(String, u8)>(params) {
                Ok((block, 0)) => state.find(&block).map_or(BLOCK_NOT_FOUND, |bytes| {
                    Reply::Result(serde_json::to_value(HexData(bytes.clone())).unwrap())
                }),
                Ok(_) => METHOD_NOT_FOUND,
                Err(_) => INVALID_PARAMS,
            },
            _ => METHOD_NOT_FOUND,
        }
    }
}

const METHOD_NOT_FOUND: Reply =
    Reply::Error { code: RpcCode::METHOD_NOT_FOUND, message: "Method not found" };
const INVALID_PARAMS: Reply =
    Reply::Error { code: RpcCode::INVALID_PARAMS, message: "Invalid params" };

const NO_TIP: Reply =
    Reply::Error { code: RpcCode::IN_WARMUP, message: "no chain tip available yet" };
const ABOVE_TIP: Reply = Reply::Error {
    code: RpcCode::INVALID_PARAMS,
    message: "Provided index is greater than the current tip",
};
const BLOCK_NOT_FOUND: Reply =
    Reply::Error { code: RpcCode::INVALID_PARAMETER, message: "Block not found" };

impl ChainState {
    /// The hash and the bytes of the block at `height`.
    fn at(&self, height: u32) -> Option<&(block::Hash, Vec<u8>)> {
        self.blocks.get(usize::try_from(height).ok()?)
    }

    /// The bytes of the block that `block` names, as zakura parses a hash or a height.
    fn find(&self, block: &str) -> Option<&Vec<u8>> {
        let tip = self.blocks.len().checked_sub(1).and_then(|tip| u32::try_from(tip).ok());
        match HashOrHeight::new(block, tip.map(block::Height)).ok()? {
            HashOrHeight::Height(height) => self.at(height.0),
            HashOrHeight::Hash(hash) => {
                self.blocks.iter().find(|(candidate, _)| *candidate == hash)
            }
        }
        .map(|(_, bytes)| bytes)
    }

    /// The `getblockchaininfo` answer of a mainnet zakura, with the tip and the prune fields of
    /// this chain.
    fn blockchain_info(&self) -> Reply {
        let Some((hash, _)) = self.blocks.last() else {
            return NO_TIP;
        };
        let tip = self.blocks.len() - 1;
        let mut info: Value =
            serde_json::from_str(include_str!("../tests/vectors/getblockchaininfo.json")).unwrap();
        info["blocks"] = json!(tip);
        info["headers"] = json!(tip);
        info["estimatedheight"] = json!(tip);
        info["bestblockhash"] = json!(hash.to_string());
        info["pruned"] = json!(self.prune_height.is_some());
        if let Some(height) = self.prune_height {
            info["pruneheight"] = json!(height.0);
        }
        Reply::Result(info)
    }
}

fn hash_reply(hash: block::Hash) -> Reply {
    Reply::Result(serde_json::to_value(GetBlockHashResponse::new(hash)).unwrap())
}
