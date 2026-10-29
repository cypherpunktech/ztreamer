//! Typed JSON-RPC calls to a remote zakura node.
//!
//! In the remote mode, `ztreamerd` runs no embedded node. It imports sealed ranges from a
//! snapshot or a peer, and gets all other chain data over zakura's JSON-RPC listener: chain
//! tip, raw blocks, tree state, mempool, transaction broadcast. [`ZakuraClient`] adds typed calls
//! for the methods that `ztreamerd` uses, with the request and response types of `zakura_rpc`.
//! [`frontier`] reads the tree states of `z_gettreestate`.
//!
//! The transport is `ztreamer_jsonrpc`. This crate re-exports its items, so a caller needs only
//! this crate.
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

mod fetch_at_height;
pub mod frontier;
mod methods;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

pub use fetch_at_height::FetchAtHeightError;
pub use methods::{Method, ZakuraClient};
pub use ztreamer_jsonrpc::{
    Auth, HttpsClient, RetryPolicy, RpcClient, RpcCode, RpcConfig, RpcError, RpcErrorObject, Secret,
};
