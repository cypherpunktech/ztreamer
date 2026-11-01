//! JSON-RPC 2.0 client over HTTP and HTTPS.
//!
//! [`RpcClient`] calls a JSON-RPC method by name. It knows nothing about the methods. The typed
//! calls of a zakura node are in `ztreamer_remote::ZakuraClient`, which uses this crate.
//!
//! What it handles:
//!
//! - HTTP and HTTPS over hyper with a rustls connector and the web PKI roots.
//! - zakura's cookie auth (the `.cookie` file the node writes) and plain basic auth.
//! - The JSON-RPC 2.0 envelope, request ids, and the node's error object.
//! - A per-method timeout with a client-wide default.
//! - Bounded retries with exponential backoff for failures that are safe to retry.
//!
//! ```no_run
//! use http::Uri;
//! use ztreamer_jsonrpc::{Auth, RpcClient, RpcConfig};
//!
//! # async fn demo() -> Result<(), ztreamer_jsonrpc::RpcError> {
//! let url = Uri::from_static("http://127.0.0.1:8232");
//! let client = RpcClient::new(
//!     RpcConfig::new(url).with_auth(Auth::Cookie("/var/lib/zakura/.cookie".into())),
//! )?;
//! let best: String = client.call("getbestblockhash", ()).await?;
//! # let _ = best;
//! # Ok(())
//! # }
//! ```

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

mod auth;
mod client;
mod envelope;
mod error;
mod https;

pub use auth::{Auth, Secret};
pub use client::{Retries, RetryPolicy, RpcClient, RpcConfig};
pub use error::{NodeState, RpcCode, RpcError, RpcErrorObject};
pub use https::HttpsClient;
