//! Sources over a remote zakura node, for a `ztreamerd` that runs without an embedded node.
//!
//! # Architecture
//!
//! In remote mode, no node runs in the `ztreamerd` process. Three parts read one remote zakura
//! node over JSON-RPC. Each part holds a [`ZakuraClient`] from `ztreamer-remote`.
//!
//! - [`RemoteTailSource`] and [`RemoteHeadSource`] read blocks for the two parts that write the
//!   compact index: [`HistoricalPipeline`] and the head follower. They replace the embedded node
//!   as the sources of these writers. The sources themselves do not write the index.
//! - [`RemoteBackend`] does not write the index. It answers the wallet requests that the index
//!   cannot answer, at the time of the request. It replaces [`InProcessBackend`] as the
//!   [`StateBackend`] of [`CompactService`].
//!
//! ```text
//!   wallet ---- gRPC ----> CompactService
//!                            |          |
//!            blocks and      |          |  tree states, subtrees, transactions,
//!            block ranges    |          |  addresses, mempool, submit
//!                            v          v
//!                     compact index    RemoteBackend -------------------------+
//!                        (LMDB)                                               |
//!                          ^                                                  |
//!        writes blocks     |     writes blocks        writes sealed ranges    |
//!        +-----------------+--------------+---------------------+             |
//!        |                                |                     |             |
//!   HistoricalPipeline              head follower           importer          |
//!   (sync_until_stable)             (follow_head)           (planned)         |
//!        ^                                ^                     ^             |
//!        | blocks                         | blocks              | ranges      |
//!   RemoteTailSource               RemoteHeadSource        snapshot hosts     |
//!        |                                |                                   |
//!        +----------------+---------------+-----------------------------------+
//!                         |   ZakuraClient (ztreamer-remote)
//!                         v
//!               remote zakura node (JSON-RPC)
//! ```
//!
//! # Who writes the index
//!
//! Three parts write the compact index. Each part writes its own band of heights:
//!
//! - **[`HistoricalPipeline`]** (`ztreamer-indexer`) writes finalized blocks in bulk. Its workers
//!   read and parse the blocks of a [`HistoricalSource`], and the pipeline writes the records in
//!   height order. `ztreamerd` runs it at startup with [`HistoricalPipeline::sync_until_stable`],
//!   which stops when two passes agree on the tip. Its source is the embedded `ZakuraDb` today
//!   and [`RemoteTailSource`] in remote mode.
//! - **The head follower** ([`CompactService::follow_head`]) writes the newest blocks and
//!   replaces them on a reorg. Its source is the embedded node today and [`RemoteHeadSource`] in
//!   remote mode.
//! - **The importer** (planned, not written yet) downloads the sealed ranges of a snapshot from
//!   snapshot hosts, verifies them against the node, and writes them with
//!   [`Index::install_sealed_range`]. Each range goes in with its [`RangeMeta`]. The importer does
//!   not use the block pipeline. It comes with the `--replicate-from` startup path of the
//!   replication plan.
//!
//! # Who writes which heights
//!
//! The writers share the chain by height. `S` is the tip of the index when the run starts.
//! `B` is the best height of the node.
//!
//! ```text
//!   height: 0 ............... S ..................... B - SEAL_DEPTH .......... B
//!           |  importer, or   |  HistoricalPipeline   |  head follower,        |
//!           |  an earlier run |  from RemoteTail-     |  from RemoteHead-      |
//!           |                 |  Source, in bulk      |  Source, with reorgs   |
//! ```
//!
//! - **Below `S`.** The index holds these blocks from an earlier run. With replication, the
//!   importer installs them as verified sealed ranges, and `S` is the sealed tip of the
//!   snapshot.
//! - **`S` to `B - SEAL_DEPTH`.** [`HistoricalPipeline`] reads from [`RemoteTailSource`], a
//!   [`HistoricalSource`]. The tip of the source is [`SEAL_DEPTH`] blocks below the best tip,
//!   because the index seals blocks at that depth. The pipeline workers read these blocks in bulk
//!   with `getblock <height> 0`, with several requests in flight for each worker. This part is
//!   long only when `S` is far behind the node.
//! - **`B - SEAL_DEPTH` to `B`.** The head follower reads from [`RemoteHeadSource`], a
//!   [`CanonicalBlockSource`]. [`CompactService::follow_head`] asks it for the tip with
//!   `getblockchaininfo`, once for each [`HeadFollowerConfig::poll_interval`], and for each new
//!   block with one `getblock <height> 0`. Before it reads blocks, it reads hashes with
//!   `getblockhash` to find the first height that changed. Thus one new block costs six
//!   requests. Each further new block in the same poll costs one request. The head follower
//!   replaces the blocks of a reorg down to [`SEAL_DEPTH`].
//!
//! With a remote source, the pipeline and the head follower write the same records as with the
//! embedded node. Thus the index does not know which mode wrote a block.
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
//! | `tree_state` | `z_gettreestate <hash>`, then `getblockchaininfo` when the block is not found | by block hash |
//! | `sapling_subtrees`, `orchard_subtrees`, `ironwood_subtrees` | `z_getsubtreesbyindex` | none |
//! | `address_transaction_ids` | `getblockcount` when the start is above 0, then `getaddresstxids` | none |
//! | `address_balance`, `address_utxos` | `getaddressbalance`, `getaddressutxos` | none |
//! | `mempool_transactions` | `getrawmempool`, then `getrawtransaction <txid> 1` | by txid |
//! | `submit_transaction` | `sendrawtransaction` | none |
//!
//! The caches keep only answers that do not change. The tree state of a block hash does not
//! change. The mempool cache keeps only the txids of the latest `getrawmempool` answer. Thus its
//! size follows the mempool of the node. `z_gettreestate` is the slowest call (0.2 to 2.6 s on
//! mainnet), and the node does not cache it.
//!
//! Each cache holds at most a budget of bytes: `TREE_CACHE_BYTES` or `MEMPOOL_CACHE_BYTES` in
//! `remote/backend.rs`. Thus the memory of the backend does not grow over time. When an insert
//! goes above the budget, the cache drops the entries that it used least recently. The bytes of
//! an entry are an estimate. Each cache reports `ztreamer.remote_cache.bytes`,
//! `ztreamer.remote_cache.entries`, `ztreamer.remote_cache.hits_total`,
//! `ztreamer.remote_cache.misses_total` and `ztreamer.remote_cache.evictions_total`, with the
//! label `cache`.
//!
//! [`RemoteBackend::connect`] refuses a node whose chain name or genesis hash is not the same as
//! the network of the index.
//!
//! # Shared parts
//!
//! - All three parts use the typed calls of [`ZakuraClient`] and the error codes on
//!   [`RpcCode`]. A block that the node does not have is [`RpcCode::INVALID_PARAMETER`], or
//!   for `getblockhash` above the tip [`RpcCode::MISC`] with a fixed message (see
//!   [`RpcError::is_block_not_found`]). A transaction that the node does not have is
//!   [`RpcCode::INVALID_ADDRESS_OR_KEY`] (see [`RpcError::is_transaction_not_found`]).
//! - Only the head follower polls the tip of the node. A mempool stream ends when the head
//!   follower publishes a new visible tip (see [`CompactService::subscribe`]). Thus the backend
//!   does not poll the tip.
//!
//! # Status
//!
//! `ztreamerd --remote-rpc <URL> --replicate-from <SOURCE>` starts no embedded node. It waits
//! until the node ends its warm-up. Then it imports the sealed ranges of the source, checked
//! against the node, and uses the three parts for the blocks above them. Before the tail, it
//! waits until the node has the tip of the index. It refuses a node of another network, and a
//! node that pruned the next block that the index needs.
//!
//! [`ZakuraClient`]: ztreamer_remote::ZakuraClient
//! [`RpcCode`]: ztreamer_remote::RpcCode
//! [`RpcCode::INVALID_PARAMETER`]: ztreamer_remote::RpcCode::INVALID_PARAMETER
//! [`RpcCode::MISC`]: ztreamer_remote::RpcCode::MISC
//! [`RpcError::is_block_not_found`]: ztreamer_remote::RpcError::is_block_not_found
//! [`RpcCode::INVALID_ADDRESS_OR_KEY`]: ztreamer_remote::RpcCode::INVALID_ADDRESS_OR_KEY
//! [`RpcError::is_transaction_not_found`]: ztreamer_remote::RpcError::is_transaction_not_found
//! [`RemoteTailSource`]: ztreamer_indexer::pipeline::RemoteTailSource
//! [`HistoricalSource`]: ztreamer_indexer::pipeline::HistoricalSource
//! [`CanonicalBlockSource`]: ztreamer_indexer::head::CanonicalBlockSource
//! [`SEAL_DEPTH`]: ztreamer_indexer::index::SEAL_DEPTH
//! [`HistoricalPipeline`]: ztreamer_indexer::pipeline::HistoricalPipeline
//! [`HistoricalPipeline::sync_until_stable`]: ztreamer_indexer::pipeline::HistoricalPipeline::sync_until_stable
//! [`Index::install_sealed_range`]: ztreamer_indexer::index::Index::install_sealed_range
//! [`RangeMeta`]: ztreamer_indexer::index::range_meta::RangeMeta
//! [`InProcessBackend`]: crate::InProcessBackend
//! [`StateBackend`]: crate::StateBackend
//! [`CompactService`]: crate::CompactService
//! [`CompactService::follow_head`]: crate::CompactService::follow_head
//! [`CompactService::subscribe`]: crate::CompactService::subscribe
//! [`HeadFollowerConfig::poll_interval`]: crate::HeadFollowerConfig::poll_interval

mod backend;
mod head;

pub use backend::{RemoteBackend, RemoteConnectError};
pub use head::RemoteHeadSource;
