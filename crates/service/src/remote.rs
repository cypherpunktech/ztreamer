//! Sources over a remote zakura node, for a `ztreamerd` that runs without an embedded node.
//!
//! # Architecture
//!
//! In remote mode, no node runs in the `ztreamerd` process. Three parts read one remote zakura
//! node over JSON-RPC. Each part holds a [`ZakuraClient`] from `ztreamer-remote`.
//!
//! - [`RemoteTailSource`] and [`RemoteHeadSource`] write blocks into the compact index. They
//!   replace the embedded node as the sources of the historical pipeline and of the head
//!   follower.
//! - [`RemoteBackend`] does not write the index. It answers the wallet requests that the index
//!   cannot answer, at the time of the request. It replaces [`InProcessBackend`] as the
//!   [`StateBackend`] of [`CompactService`].
//!
//! ```text
//!   wallet ---- gRPC ----> CompactService
//!                            |          |
//!            blocks and      |          |  tree states, subtrees, transactions,
//!            block ranges    |          |  addresses, mempool, submit, tip changes
//!                            v          v
//!                     compact index    RemoteBackend -----------------+
//!                        (LMDB)                                       |
//!                          ^                                          |
//!                          | writes blocks                            |
//!            +-------------+--------------+                           |
//!            |                            |                           |
//!     RemoteTailSource             RemoteHeadSource                   |
//!     (historical pipeline)        (head follower)                    |
//!            |                            |                           |
//!            +------------+---------------+---------------------------+
//!                         |   ZakuraClient (ztreamer-remote)
//!                         v
//!               remote zakura node (JSON-RPC)
//! ```
//!
//! # Who writes which heights
//!
//! The two sources share the chain by height. `S` is the tip of the index when the run starts.
//! `B` is the best height of the node.
//!
//! ```text
//!   height: 0 ............... S ..................... B - SEAL_DEPTH .......... B
//!           |   already in    |    RemoteTailSource   |    RemoteHeadSource    |
//!           |   the index     |    bulk, finalized    |    follows the tip,    |
//!           |                 |    blocks only        |    replaces reorgs     |
//! ```
//!
//! - **Below `S`.** The index holds these blocks from an earlier run. With replication, the
//!   import of a snapshot installs them as verified sealed ranges, and `S` is the sealed tip of
//!   the snapshot.
//! - **`S` to `B - SEAL_DEPTH`.** [`RemoteTailSource`] is a [`HistoricalSource`]. Its tip is
//!   [`SEAL_DEPTH`] blocks below the best tip, because the index seals blocks at that depth. The
//!   pipeline workers read these blocks in bulk with `getblock <height> 0`, with several requests
//!   in flight for each worker. This part is long only when `S` is far behind the node.
//! - **`B - SEAL_DEPTH` to `B`.** [`RemoteHeadSource`] is a [`CanonicalBlockSource`].
//!   [`CompactService::follow_head`] asks it for the tip with `getblockchaininfo`, once for each
//!   [`HeadFollowerConfig::poll_interval`], and for each new block with `getblockhash` and then
//!   `getblock <hash> 0`. The head follower replaces the blocks of a reorg down to
//!   [`SEAL_DEPTH`].
//!
//! The two sources write the same records as the embedded node writes. Thus the index does not
//! know which mode wrote a block.
//!
//! # What the backend answers
//!
//! [`CompactService`] answers block and block range requests from the index. For all other
//! requests it calls its [`StateBackend`]. [`RemoteBackend`] calls the node for each of them and
//! parses the answer into the `zakura_chain` type of the trait:
//!
//! | `StateBackend` method | zakura call | Cache |
//! |---|---|---|
//! | `tip_height` | `getblockcount` | none |
//! | `block` | `getblock <hash> 0` | none |
//! | `transaction` | `getrawtransaction <txid> 1` | none |
//! | `tree_state` | `z_gettreestate <hash>` | by block hash, 16 MiB |
//! | `sapling_subtrees`, `orchard_subtrees`, `ironwood_subtrees` | `z_getsubtreesbyindex` | by index, when final, 2 MiB per pool |
//! | `address_transaction_ids`, `address_balance`, `address_utxos` | `getaddresstxids`, `getaddressbalance`, `getaddressutxos` | none |
//! | `mempool_transactions` | `getrawmempool`, then `getrawtransaction <txid> 1` | by txid, 160 MiB |
//! | `submit_transaction` | `sendrawtransaction` | none |
//! | `tip_listener` | `getbestblockhash`, from the [`TipPoller`] | none |
//!
//! The caches keep only answers that do not change. The tree state of a block hash does not
//! change. A subtree goes in the cache only when it is at least
//! [`MAX_BLOCK_REORG_HEIGHT`](zakura_state::MAX_BLOCK_REORG_HEIGHT) blocks below the tip. The
//! mempool cache keeps only the txids of the latest `getrawmempool` answer, so its size follows
//! the mempool of the node. `z_gettreestate` is the slowest call (0.2 to 2.6 s on mainnet), and
//! the node does not cache it.
//!
//! Each cache holds at most the bytes in the table, so the memory of the backend does not grow
//! over time: about 182 MiB in all, and normally much less. When an insert goes above the
//! budget, the cache drops the entries that were used least recently. The bytes of an entry are
//! an estimate. Each cache reports `ztreamer.remote_cache.bytes`,
//! `ztreamer.remote_cache.entries`, `ztreamer.remote_cache.hits_total`,
//! `ztreamer.remote_cache.misses_total` and `ztreamer.remote_cache.evictions_total`, with the
//! label `cache`.
//!
//! [`RemoteBackend::connect`] refuses a node whose chain name or genesis hash is not the same as
//! the network of the index. It also returns the [`TipPoller`]. The caller spawns
//! [`TipPoller::run`], which calls `getbestblockhash` only while a tip listener exists.
//!
//! # Shared parts
//!
//! - All three parts use the typed calls of [`ZakuraClient`] and the error codes on
//!   [`RpcCode`]. A block that the node does not have is [`RpcCode::INVALID_PARAMETER`]. A
//!   transaction that the node does not have is [`RpcCode::INVALID_ADDRESS_OR_KEY`].
//! - The head follower and the [`TipPoller`] both poll the tip of the node: the first with
//!   `getblockchaininfo`, the second with `getbestblockhash`. While a mempool stream is open,
//!   the node thus gets two tip calls per second for the same tip. One shared tip watch could
//!   feed both.
//!
//! # Status
//!
//! The three parts are ready, but `ztreamerd` does not use them yet: it runs only with the
//! embedded node. The remote mode of the daemon, its configuration and the snapshot import come
//! in later PRs of the replication plan.
//!
//! [`ZakuraClient`]: ztreamer_remote::ZakuraClient
//! [`RpcCode`]: ztreamer_remote::RpcCode
//! [`RpcCode::INVALID_PARAMETER`]: ztreamer_remote::RpcCode::INVALID_PARAMETER
//! [`RpcCode::INVALID_ADDRESS_OR_KEY`]: ztreamer_remote::RpcCode::INVALID_ADDRESS_OR_KEY
//! [`RemoteTailSource`]: ztreamer_indexer::pipeline::RemoteTailSource
//! [`HistoricalSource`]: ztreamer_indexer::pipeline::HistoricalSource
//! [`CanonicalBlockSource`]: ztreamer_indexer::head::CanonicalBlockSource
//! [`SEAL_DEPTH`]: ztreamer_indexer::index::SEAL_DEPTH
//! [`InProcessBackend`]: crate::InProcessBackend
//! [`StateBackend`]: crate::StateBackend
//! [`CompactService`]: crate::CompactService
//! [`CompactService::follow_head`]: crate::CompactService::follow_head
//! [`HeadFollowerConfig::poll_interval`]: crate::HeadFollowerConfig::poll_interval

mod backend;
mod head;

pub use backend::{RemoteBackend, RemoteConnectError, TipPoller};
pub use head::RemoteHeadSource;
