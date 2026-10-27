//! JSON-RPC client for a remote zakura node.
//!
//! In the remote mode, `ztreamerd` runs no embedded node. It imports sealed ranges from a
//! snapshot or a peer, and gets all other chain data over zakura's JSON-RPC listener: chain
//! tip, raw blocks, tree state, mempool, transaction broadcast. This crate is the transport for
//! those calls. [`RpcClient`] knows nothing about the methods. [`ZakuraClient`] adds typed calls
//! for the methods that `ztreamerd` uses, with the request and response types of `zakura_rpc`.
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

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

mod auth;
mod client;
mod envelope;
mod error;
mod methods;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

pub use auth::{Auth, Secret};
pub use client::{RetryPolicy, RpcClient, RpcConfig};
pub use error::{RpcCode, RpcError, RpcErrorObject};
pub use methods::{Method, ZakuraClient};
