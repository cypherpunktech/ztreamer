//! Sources over a remote zakura node, for a `ztreamerd` that runs without an embedded node.
//!
//! In remote mode, `ztreamerd` first imports the sealed ranges of `--replicate-from`, and each
//! epoch is checked against the `--remote-rpc` node. Then it follows that node. Three parts read
//! the node over JSON-RPC with a [`ZakuraClient`]. `S` is the tip of the index after the import,
//! and `B` is the best height of the node:
//!
//! - [`RemoteTailSource`] gives the blocks from `S` to `B - SEAL_DEPTH` to the
//!   [`HistoricalPipeline`], which writes them in bulk.
//! - [`RemoteHeadSource`] gives the blocks from `B - SEAL_DEPTH` to `B` to the head follower
//!   ([`CompactService::follow_head`]), which replaces them on a reorg.
//! - [`RemoteBackend`] is the [`StateBackend`] of [`CompactService`]. It answers the requests
//!   that the index cannot answer, with one call to the node for each:
//!
//! | `StateBackend` method | zakura call |
//! |---|---|
//! | `tip_height` | `getblockcount` |
//! | `block` | `getblock <hash> 0` |
//! | `transaction` | `getrawtransaction <txid> 1` |
//! | `tree_state` | `z_gettreestate <hash>`, then `getblockchaininfo` when the block is not found |
//! | `sapling_subtrees`, `orchard_subtrees`, `ironwood_subtrees` | `z_getsubtreesbyindex` |
//! | `address_transaction_ids` | `getblockcount` when the start is above 0, then `getaddresstxids` |
//! | `address_balance`, `address_utxos` | `getaddressbalance`, `getaddressutxos` |
//! | `mempool_transactions` | `getrawmempool`, then `getrawtransaction <txid> 1` |
//! | `submit_transaction` | `sendrawtransaction` |
//!
//! The [`RangeMetaFiller`] also uses the `RemoteBackend`. For each range that the tail or the
//! head follower seals, it calls `tree_state` and the subtree methods, and writes the
//! `range_meta` row. Thus the Replicator and `export-snapshot` serve each sealed range of a
//! replica, not only the imported ranges.
//!
//! [`ZakuraClient`]: ztreamer_remote::ZakuraClient
//! [`RangeMetaFiller`]: crate::RangeMetaFiller
//! [`RemoteTailSource`]: ztreamer_indexer::pipeline::RemoteTailSource
//! [`HistoricalPipeline`]: ztreamer_indexer::pipeline::HistoricalPipeline
//! [`StateBackend`]: crate::StateBackend
//! [`CompactService`]: crate::CompactService
//! [`CompactService::follow_head`]: crate::CompactService::follow_head

mod backend;
mod head;

pub use backend::{RemoteBackend, RemoteConnectError};
pub use head::RemoteHeadSource;
