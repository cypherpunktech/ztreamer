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
//!            block ranges    |          |  addresses, mempool, submit, tip changes
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
//!   block with `getblockhash` and then `getblock <hash> 0`. Before it reads blocks, it reads
//!   hashes with `getblockhash` to find the first height that changed, thus one new block costs
//!   seven requests. The head follower replaces the blocks of a reorg down to [`SEAL_DEPTH`].
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
//!   [`RpcCode`]. A block that the node does not have is [`RpcCode::INVALID_PARAMETER`], or
//!   for `getblockhash` above the tip [`RpcCode::MISC`] with a fixed message (see
//!   [`RpcError::is_block_not_found`]). A transaction that the node does not have is
//!   [`RpcCode::INVALID_ADDRESS_OR_KEY`].
//! - The head follower and the [`TipPoller`] both poll the tip of the node: the first with
//!   `getblockchaininfo`, the second with `getbestblockhash`. While a mempool stream is open,
//!   the node thus gets two tip calls per second for the same tip. One shared tip watch could
//!   feed both.
//!
//! # Status
//!
//! `ztreamerd --remote-rpc <URL>` uses the three parts and starts no embedded node. Before the
//! first sync, it waits until the node ends its warm-up and has the tip of the index. It refuses
//! a node of another network, and a node that pruned the next block that the index needs. The
//! snapshot import comes in a later PR of the replication plan.
//!
//! [`ZakuraClient`]: ztreamer_remote::ZakuraClient
//! [`RpcCode`]: ztreamer_remote::RpcCode
//! [`RpcCode::INVALID_PARAMETER`]: ztreamer_remote::RpcCode::INVALID_PARAMETER
//! [`RpcCode::MISC`]: ztreamer_remote::RpcCode::MISC
//! [`RpcError::is_block_not_found`]: ztreamer_remote::RpcError::is_block_not_found
//! [`RpcCode::INVALID_ADDRESS_OR_KEY`]: ztreamer_remote::RpcCode::INVALID_ADDRESS_OR_KEY
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
//! [`HeadFollowerConfig::poll_interval`]: crate::HeadFollowerConfig::poll_interval

mod backend;
mod head;

pub use backend::{RemoteBackend, RemoteConnectError, TipPoller};
pub use head::RemoteHeadSource;
