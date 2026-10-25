//! A [`HistoricalSource`] over the JSON-RPC interface of a remote zakura node.

use std::{collections::VecDeque, num::NonZeroUsize, panic, time::Instant};

use tokio::{runtime::Handle, task::JoinHandle};
use tracing::warn;
use zakura_chain::block::{self, Height};
use zakura_state::{HashOrHeight, MAX_BLOCK_REORG_HEIGHT};
use ztreamer_remote::{RpcError, ZakuraClient};

use super::{HistoricalSource, PipelineError, WorkerStats, flow_control::SegmentSender};
use crate::{parser::parse_block, types::RawIndexBlock};

/// The `getblock` requests that one worker keeps in flight when it does not set another value.
pub const DEFAULT_REQUESTS_IN_FLIGHT: NonZeroUsize = NonZeroUsize::new(8).unwrap();

/// The finalized chain of a remote zakura node, read over JSON-RPC.
///
/// zakura finalizes a block when it is [`MAX_BLOCK_REORG_HEIGHT`] blocks below the best tip. This
/// source reports that height as its tip. The head follower reads the blocks above it.
///
/// # Runtime
///
/// The pipeline calls the [`HistoricalSource`] methods on blocking worker threads. This source
/// holds a [`Handle`] and uses [`Handle::block_on`] for each request. Thus:
///
/// - The handle must be of a multi-thread runtime. On a current-thread runtime, only
///   `Runtime::block_on` drives the IO, and the requests do not complete.
/// - Run the pipeline on a blocking thread, for example in `tokio::task::spawn_blocking` or on a
///   `std::thread`, never in an async task. [`Handle::block_on`] panics in an async context.
///
/// [`HistoricalSource::process_segment`] spawns the `getblock` requests on the runtime, up to
/// [`Self::with_requests_in_flight`] ahead of the block that the worker waits for. The requests
/// continue while the worker parses and sends a block, and while it waits for read-ahead budget.
/// The node thus gets up to [`PipelineConfig::workers`](super::PipelineConfig::workers) times
/// this number of `getblock` requests at the same time.
///
/// ```no_run
/// use ztreamer_indexer::pipeline::{HistoricalPipeline, PipelineConfig, RemoteTailSource};
/// # use ztreamer_indexer::index::Index;
/// use ztreamer_remote::{RpcClient, ZakuraClient};
///
/// # async fn demo(index: std::sync::Arc<Index>) -> Result<(), Box<dyn std::error::Error>> {
/// let config = ZakuraClient::config("http://127.0.0.1:8232".parse()?);
/// let source =
///     RemoteTailSource::new(ZakuraClient::new(RpcClient::new(config)?), tokio::runtime::Handle::current());
/// let state = tokio::task::spawn_blocking(move || {
///     HistoricalPipeline::new(&index, &source, PipelineConfig::default())?.sync_until_stable()
/// })
/// .await??;
/// # let _ = state;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct RemoteTailSource {
    node: ZakuraClient,
    runtime: Handle,
    requests_in_flight: NonZeroUsize,
}

impl RemoteTailSource {
    /// A source that reads from `node` with requests on `runtime`.
    pub const fn new(node: ZakuraClient, runtime: Handle) -> Self {
        Self { node, runtime, requests_in_flight: DEFAULT_REQUESTS_IN_FLIGHT }
    }

    /// Sets the number of `getblock` requests that each worker keeps in flight. The default is
    /// [`DEFAULT_REQUESTS_IN_FLIGHT`].
    pub const fn with_requests_in_flight(mut self, requests: NonZeroUsize) -> Self {
        self.requests_in_flight = requests;
        self
    }

    async fn finalized_tip(&self) -> Result<(Height, block::Hash), RpcError> {
        let info = self.node.get_blockchain_info().await?;
        let best = info.blocks();
        let finalized = Height(best.0.saturating_sub(MAX_BLOCK_REORG_HEIGHT));
        if finalized == best {
            return Ok((best, info.best_block_hash()));
        }
        Ok((finalized, self.node.get_block_hash(finalized).await?.hash()))
    }
}

impl HistoricalSource for RemoteTailSource {
    /// The best height minus [`MAX_BLOCK_REORG_HEIGHT`], or genesis on a shorter chain. `None`
    /// when a request fails.
    fn tip(&self) -> Option<(Height, block::Hash)> {
        self.runtime
            .block_on(self.finalized_tip())
            .inspect_err(|error| warn!(%error, "remote tip request failed"))
            .ok()
    }

    /// The hash from `getblockhash`. `None` when the node has no block at `height` or the
    /// request fails.
    fn block_hash(&self, height: u32) -> Option<block::Hash> {
        self.runtime
            .block_on(self.node.get_block_hash(Height(height)))
            .inspect_err(|error| {
                if !error.is_block_not_found() {
                    warn!(%error, height, "remote block hash request failed");
                }
            })
            .ok()
            .map(|hash| hash.hash())
    }

    /// The `pruneheight` field of `getblockchaininfo`. Zero when the node does not prune or the
    /// request fails.
    fn prune_height(&self) -> u32 {
        self.runtime
            .block_on(self.node.get_blockchain_info())
            .inspect_err(|error| warn!(%error, "remote prune height request failed"))
            .ok()
            .and_then(|info| info.prune_height())
            .map_or(0, |height| height.0)
    }

    fn process_segment(
        &self,
        start: u32,
        end: u32,
        expected_end_hash: Option<block::Hash>,
        max_source_bytes: usize,
        parsed_tx: &mut SegmentSender<'_>,
        stats: &mut WorkerStats,
    ) -> Result<(), PipelineError> {
        let mut requests = BlockRequests::new(self, start, end);
        stats.ranges = 1;
        for height in start..=end {
            let started = Instant::now();
            let bytes = requests.next(height);
            stats.read += started.elapsed();
            let bytes = bytes?;
            // The node sent the whole block. Refuse a block that is already too large before
            // it is decoded.
            if bytes.len() > max_source_bytes {
                return Err(PipelineError::BlockExceedsByteLimit {
                    height,
                    required: bytes.len(),
                    limit: max_source_bytes,
                });
            }

            let started = Instant::now();
            let block = RawIndexBlock::from_bytes(bytes)
                .map_err(|source| PipelineError::BlockDecode { height, source })?;
            if block.height.0 != height {
                return Err(PipelineError::SourceGap {
                    expected: height,
                    actual: Some(block.height.0),
                });
            }
            let required = block
                .txids
                .len()
                .checked_mul(32)
                .and_then(|txids| txids.checked_add(block.bytes.len()))
                .ok_or(PipelineError::SourceSize { height })?;
            if required > max_source_bytes {
                return Err(PipelineError::BlockExceedsByteLimit {
                    height,
                    required,
                    limit: max_source_bytes,
                });
            }
            if height == end && expected_end_hash.is_some_and(|hash| hash != block.hash) {
                return Err(PipelineError::SourceChanged);
            }
            let parsed = parse_block(&block);
            stats.parse += started.elapsed();
            let parsed = parsed?;
            stats.blocks += 1;
            stats.transactions += block.txids.len() as u64;
            stats.bytes += block.bytes.len() as u64;

            let started = Instant::now();
            let sent = parsed_tx.send(parsed);
            stats.send_wait += started.elapsed();
            sent?;
        }
        Ok(())
    }
}

/// The `getblock` requests of one segment, in height order. Up to the source's
/// `requests_in_flight` run on the runtime ahead of the block that the worker waits for. When
/// this value is dropped, it aborts the requests that are left, for example after an error.
struct BlockRequests<'a> {
    source: &'a RemoteTailSource,
    next: u32,
    end: u32,
    pending: VecDeque<JoinHandle<Result<Vec<u8>, RpcError>>>,
}

impl<'a> BlockRequests<'a> {
    fn new(source: &'a RemoteTailSource, start: u32, end: u32) -> Self {
        let mut requests = Self { source, next: start, end, pending: VecDeque::new() };
        requests.fill();
        requests
    }

    fn fill(&mut self) {
        while self.pending.len() < self.source.requests_in_flight.get() && self.next <= self.end {
            let node = self.source.node.clone();
            let height = Height(self.next);
            self.pending.push_back(self.source.runtime.spawn(async move {
                node.get_block_raw(HashOrHeight::Height(height)).await.map(|bytes| bytes.0)
            }));
            self.next += 1;
        }
    }

    /// Waits for the serialized block at `height`, the next height of the segment. A block that
    /// the node does not have is [`PipelineError::SourceGap`].
    fn next(&mut self, height: u32) -> Result<Vec<u8>, PipelineError> {
        let request = self.pending.pop_front().expect("the segment has a request for each height");
        self.fill();
        match self.source.runtime.block_on(request) {
            Ok(Ok(bytes)) => Ok(bytes),
            Ok(Err(error)) if error.is_block_not_found() => {
                Err(PipelineError::SourceGap { expected: height, actual: None })
            }
            Ok(Err(error)) => Err(error.into()),
            Err(error) if error.is_panic() => panic::resume_unwind(error.into_panic()),
            // The runtime shut down and dropped the request.
            Err(error) => {
                Err(RpcError::Transport { method: "getblock".to_owned(), source: Box::new(error) }
                    .into())
            }
        }
    }
}

impl Drop for BlockRequests<'_> {
    fn drop(&mut self) {
        self.pending.iter().for_each(JoinHandle::abort);
    }
}

#[cfg(test)]
mod tests {
    use tokio::runtime::{Builder, Runtime};
    use zakura_chain::{block::Block, serialization::ZcashSerialize as _};
    use ztreamer_remote::{
        RpcClient, RpcCode,
        test_utils::{MockChain, MockNode, Reply, Seen},
    };

    use super::*;
    use crate::{
        index::{BlockId, Index},
        pipeline::{HistoricalPipeline, PipelineConfig},
        test_utils::grow_chain,
    };

    fn runtime() -> Runtime {
        Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap()
    }

    /// A best chain whose finalized tip is at height `finalized`.
    fn chain(finalized: u32) -> Vec<Block> {
        let mut chain = Vec::new();
        grow_chain(&mut chain, finalized + MAX_BLOCK_REORG_HEIGHT, 0);
        chain
    }

    /// A mock node with `script` on `runtime`, and a source over it.
    fn node(
        runtime: &Runtime,
        script: impl Fn(usize, &Seen) -> Reply + Send + Sync + 'static,
    ) -> (MockNode, RemoteTailSource) {
        let mock = runtime.block_on(MockNode::start(script));
        let node = ZakuraClient::new(RpcClient::new(mock.config()).unwrap());
        (mock, RemoteTailSource::new(node, runtime.handle().clone()))
    }

    fn serve(chain: &MockChain) -> impl Fn(usize, &Seen) -> Reply + Send + Sync + 'static {
        let chain = chain.clone();
        move |_, request| chain.reply(request)
    }

    fn index(dir: &tempfile::TempDir) -> Index {
        Index::open(dir.path(), 64 * 1024 * 1024, "Mainnet", block::Hash([9; 32])).unwrap()
    }

    fn config() -> PipelineConfig {
        PipelineConfig { workers: 4, source_segment_blocks: 64, ..PipelineConfig::default() }
    }

    fn size(block: &Block) -> usize {
        block.zcash_serialize_to_vec().unwrap().len()
    }

    #[test]
    fn reads_the_finalized_tip_and_the_prune_height() {
        let runtime = runtime();
        let blocks = chain(100);
        let chain = MockChain::new(&blocks);
        let (_mock, source) = node(&runtime, serve(&chain));

        assert_eq!(source.tip(), Some((Height(100), blocks[100].hash())));
        assert_eq!(source.block_hash(7), Some(blocks[7].hash()));
        assert_eq!(source.block_hash(5_000), None);
        assert_eq!(source.prune_height(), 0);
        chain.set_prune_height(Some(Height(40)));
        assert_eq!(source.prune_height(), 40);

        // A chain shorter than the reorg window has only genesis finalized.
        chain.set_blocks(&blocks[..=10]);
        assert_eq!(source.tip(), Some((Height(0), blocks[0].hash())));
    }

    #[test]
    fn syncs_two_thousand_blocks() {
        let runtime = runtime();
        let blocks = chain(1_999);
        let (_mock, source) = node(&runtime, serve(&MockChain::new(&blocks)));
        let dir = tempfile::tempdir().unwrap();
        let index = index(&dir);

        let state = HistoricalPipeline::new(&index, &source, config())
            .unwrap()
            .sync_until_stable()
            .unwrap();

        assert_eq!(state.durable_tip(), Some(BlockId::new(1_999, blocks[1_999].hash())));
        let record = index.read_block(state.generation(), 1_234).unwrap();
        assert_eq!(record.hash, blocks[1_234].hash());
        assert_eq!(record.previous_hash, blocks[1_233].hash());
        index.verify_continuity().unwrap();
    }

    #[test]
    fn a_gap_in_the_answers_ends_the_pass() {
        let runtime = runtime();
        let serve = serve(&MockChain::new(&chain(300)));
        let (_mock, source) = node(&runtime, move |n, request| {
            if request.body["method"] == "getblock" && request.body["params"][0] == "150" {
                return Reply::Error {
                    code: RpcCode::INVALID_PARAMETER,
                    message: "Block not found",
                };
            }
            serve(n, request)
        });
        let dir = tempfile::tempdir().unwrap();
        let index = index(&dir);

        let result = HistoricalPipeline::new(&index, &source, config()).unwrap().sync();

        assert!(
            matches!(result, Err(PipelineError::SourceGap { expected: 150, actual: None })),
            "{result:?}"
        );
    }

    #[test]
    fn a_block_above_the_byte_limit_is_refused() {
        let runtime = runtime();
        let blocks = chain(300);
        // Genesis is the largest block of the chain. One transaction id adds 32 bytes.
        let limit = size(&blocks[0]) + 32 - 1;
        assert!(blocks[1..].iter().all(|block| size(block) + 32 <= limit));
        let (_mock, source) = node(&runtime, serve(&MockChain::new(&blocks)));
        let dir = tempfile::tempdir().unwrap();
        let index = index(&dir);

        let result = HistoricalPipeline::new(
            &index,
            &source,
            PipelineConfig { max_source_bytes: limit, ..config() },
        )
        .unwrap()
        .sync();

        assert!(
            matches!(
                result,
                Err(PipelineError::BlockExceedsByteLimit { height: 0, required, limit: refused })
                    if required == limit + 1 && refused == limit
            ),
            "{result:?}"
        );
    }
}
