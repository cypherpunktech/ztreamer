//! JSON-RPC client for a remote zakura node.
//!
//! `ztreamerd` can run without an embedded node. Everything it then needs from the chain comes
//! over zakura's JSON-RPC listener: chain tip, raw blocks, tree state, mempool, transaction
//! broadcast. This crate is the transport for those calls. It knows nothing about the methods
//! themselves; typed wrappers live one layer up.
//!
//! What it handles:
//!
//! - HTTP and HTTPS over hyper with a rustls connector (web PKI roots plus an optional extra CA
//!   file for self-signed node certificates).
//! - zakura's cookie auth (the `.cookie` file the node writes) and plain basic auth.
//! - The JSON-RPC 2.0 envelope, request ids, and the node's error object.
//! - A per-method timeout with a client-wide default.
//! - Bounded retries with exponential backoff for failures that are safe to retry.
//!
//! ```no_run
//! use http::Uri;
//! use ztreamer_remote::{Auth, RpcClient, RpcConfig};
//!
//! # async fn demo() -> Result<(), ztreamer_remote::RpcError> {
//! let url = Uri::from_static("http://127.0.0.1:8232");
//! let client = RpcClient::new(
//!     RpcConfig::new(url).with_auth(Auth::Cookie("/var/lib/zakura/.cookie".into())),
//! )?;
//! let best: String = client.call("getbestblockhash", ()).await?;
//! # let _ = best;
//! # Ok(())
//! # }
//! ```

mod auth;
mod client;
mod envelope;
mod error;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

pub use auth::{Auth, Secret, read_cookie};
pub use client::{CallOptions, RetryPolicy, RpcClient, RpcConfig};
pub use error::{RpcCode, RpcError, RpcErrorObject};
