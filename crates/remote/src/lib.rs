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
//!
//! # The remote mode
//!
//! In remote mode, `ztreamerd` first imports the sealed ranges of `--replicate-from`, and each
//! epoch is checked against the `--remote-rpc` node. Then it follows that node. Three parts read
//! the node over JSON-RPC with a [`ZakuraClient`]. `S` is the tip of the index after the import,
//! and `B` is the best height of the node:
//!
//! - [`RemoteTailSource`] gives the blocks from `S` to `B - SEAL_DEPTH` to the
//!   [`HistoricalPipeline`], which writes them in bulk.
//! - [`RemoteHeadSource`] gives the blocks from `B - SEAL_DEPTH` to `B` to the head follower
//!   ([`HeadFollower::follow_head`]), which replaces them on a reorg.
//! - [`RemoteBackend`] is the [`StateBackend`] of `ztreamer_service::CompactService`. It answers
//!   the requests that the index cannot answer, with one call to the node for each:
//!
//! | `StateBackend` method | zakura call |
//! |---|---|
//! | `tip_height` | `getblockcount` |
//! | `block` | `getblock <hash> 0` |
//! | `transaction` | `getrawtransaction <txid> 1` |
//! | `tree_state` | `z_gettreestate <hash>` |
//! | `sapling_subtrees`, `orchard_subtrees`, `ironwood_subtrees` | `z_getsubtreesbyindex` |
//! | `address_transaction_ids` | `getblockcount` when the start is above 0, then `getaddresstxids` |
//! | `address_balance`, `address_utxos` | `getaddressbalance`, `getaddressutxos` |
//! | `mempool_transactions` | `getrawmempool`, then `getrawtransaction <txid> 1` |
//! | `submit_transaction` | `sendrawtransaction` |
//!
//! The `range_meta` filler of `ztreamer_replicator` also uses the `RemoteBackend`. For each range
//! that the tail or the head follower seals, it calls `tree_state` and the subtree methods, and
//! writes the `range_meta` row. Thus the Replicator and `export-snapshot` serve each sealed
//! range of a replica, not only the imported ranges.
//!
//! [`HistoricalPipeline`]: ztreamer_sync::pipeline::HistoricalPipeline
//! [`StateBackend`]: ztreamer_backend::StateBackend
//! [`HeadFollower::follow_head`]: ztreamer_sync::head::HeadFollower::follow_head

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

mod backend;
mod fetch_at_height;
pub mod frontier;
mod head;
mod methods;
mod node_chain;
pub mod node_sync;
mod node_url;
mod tail;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

pub use backend::{RemoteBackend, RemoteConnectError};
pub use fetch_at_height::FetchAtHeightError;
pub use head::RemoteHeadSource;
pub use methods::{Method, ZakuraClient};
pub use node_chain::{NodeChain, UnknownChain};
pub use node_sync::{NodeSyncWait, NodeWaitError};
pub use node_url::{NodeUrl, NodeUrlError};
pub use tail::RemoteTailSource;
pub use ztreamer_jsonrpc::{
    Auth, HttpsClient, NodeState, RetryPolicy, RpcClient, RpcCode, RpcConfig, RpcError,
    RpcErrorObject, Secret,
};
