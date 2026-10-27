//! [`CanonicalBlockSource`] over the JSON-RPC interface of a remote zakura node.

use std::panic;

use zakura_chain::block;
use zakura_state::HashOrHeight;
use ztreamer_indexer::{
    head::{CanonicalBlockSource, HeadError},
    index::BlockId,
    types::RawIndexBlock,
};
use ztreamer_remote::ZakuraClient;

/// The canonical head of a remote zakura node, read over JSON-RPC.
///
/// [`CompactService::sync_head`](crate::CompactService::sync_head) and
/// [`CompactService::follow_head`](crate::CompactService::follow_head) use it as they use the
/// embedded node. Each [`CanonicalBlockSource::block_hash`] call sends one `getblockhash`
/// request. Each [`CanonicalBlockSource::block`] call sends one `getblock` request with the
/// height and verbosity 0. The block bytes go into the index without a second serialization.
///
/// ```no_run
/// use ztreamer_remote::{RpcClient, ZakuraClient};
/// use ztreamer_service::remote::RemoteHeadSource;
///
/// # fn demo() -> Result<(), ztreamer_remote::RpcError> {
/// let config = ZakuraClient::config("http://127.0.0.1:8232".parse().unwrap());
/// let source = RemoteHeadSource::new(ZakuraClient::new(RpcClient::new(config)?));
/// # let _ = source;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct RemoteHeadSource {
    node: ZakuraClient,
}

impl RemoteHeadSource {
    /// A source that reads the head of `node`.
    pub const fn new(node: ZakuraClient) -> Self {
        Self { node }
    }
}

#[tonic::async_trait]
impl CanonicalBlockSource for RemoteHeadSource {
    /// The best block, from one `getblockchaininfo` call. The node takes the height and the hash
    /// from the same chain tip, thus they agree.
    async fn tip(&mut self) -> Result<Option<BlockId>, HeadError> {
        let info = self.node.get_blockchain_info().await?;
        Ok(Some(BlockId::new(info.blocks().0, info.best_block_hash())))
    }

    /// The best-chain block at `height`, or `None` when the node has no block at that height.
    ///
    /// It decodes the block on the blocking thread pool. The hash comes from the block. [`poll_canonical_head`] checks that each block links to
    /// the block below it. After the reads, it reads the hash of the last block again.
    ///
    /// [`poll_canonical_head`]: ztreamer_indexer::head::poll_canonical_head
    async fn block(&mut self, height: u32) -> Result<Option<RawIndexBlock>, HeadError> {
        let bytes = match self.node.get_block_raw(HashOrHeight::Height(block::Height(height))).await
        {
            Ok(bytes) => bytes.0,
            Err(error) if error.is_block_not_found() => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let block = tokio::task::spawn_blocking(move || RawIndexBlock::from_bytes(bytes))
            .await
            .unwrap_or_else(|error| panic::resume_unwind(error.into_panic()))
            .map_err(|error| HeadError::Decode(error.to_string()))?;
        if block.height.0 != height {
            return Err(HeadError::Height { expected: height, actual: block.height.0 });
        }
        Ok(Some(block))
    }

    /// The best-chain hash at `height` from one `getblockhash` call, or `None` when the node has
    /// no block at that height.
    async fn block_hash(&mut self, height: u32) -> Result<Option<block::Hash>, HeadError> {
        match self.node.get_block_hash(block::Height(height)).await {
            Ok(hash) => Ok(Some(hash.hash())),
            Err(error) if error.is_block_not_found() => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::json;
    use tokio::sync::watch;
    use zakura_chain::{block::Block, serialization::ZcashSerialize as _};
    use ztreamer_indexer::{pipeline::PipelineConfig, test_utils::grow_chain};
    use ztreamer_remote::{
        RpcClient, RpcCode,
        test_utils::{MockChain, MockNode, Reply},
    };

    use super::*;
    use crate::{CompactService, HeadFollowerConfig, test_utils::EphemeralService};

    /// A mock node that serves `chain`, and a source over it.
    async fn node(chain: &MockChain) -> (MockNode, RemoteHeadSource) {
        let mock = MockNode::start({
            let chain = chain.clone();
            move |_, request| chain.reply(request)
        })
        .await;
        let source =
            RemoteHeadSource::new(ZakuraClient::new(RpcClient::new(mock.config()).unwrap()));
        (mock, source)
    }

    fn chain(tip: u32) -> Vec<Block> {
        let mut chain = Vec::new();
        grow_chain(&mut chain, tip, 0);
        chain
    }

    fn visible_tip(service: &CompactService) -> Option<BlockId> {
        service.snapshot().visible_tip
    }

    fn id(block: &Block) -> BlockId {
        BlockId::new(block.coinbase_height().unwrap().0, block.hash())
    }

    #[tokio::test]
    async fn reads_the_tip_and_the_blocks() {
        let blocks = chain(5);
        let (_mock, mut source) = node(&MockChain::new(&blocks)).await;

        assert_eq!(source.tip().await.unwrap(), Some(id(&blocks[5])));
        let block = source.block(3).await.unwrap().unwrap();
        assert_eq!(block.height, block::Height(3));
        assert_eq!(block.hash, blocks[3].hash());
        assert_eq!(block.bytes, blocks[3].zcash_serialize_to_vec().unwrap());
        assert_eq!(block.txids, [blocks[3].transactions[0].hash()]);
        assert!(source.block(6).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn a_height_above_the_tip_of_the_node_is_no_block() {
        let blocks = chain(5);
        let chain = MockChain::new(&blocks);
        // This error has the code of the `getblockhash` error above the tip, with another message.
        let mock = MockNode::start(move |_, request| {
            match (request.body["method"].as_str(), request.body["params"][0].as_u64()) {
                (Some("getblockhash"), Some(7)) => {
                    Reply::Error { code: RpcCode::MISC, message: "Invalid params" }
                }
                _ => chain.reply(request),
            }
        })
        .await;
        let mut source =
            RemoteHeadSource::new(ZakuraClient::new(RpcClient::new(mock.config()).unwrap()));

        assert!(source.block(5).await.unwrap().is_some());
        assert!(source.block(6).await.unwrap().is_none());
        assert_eq!(source.block_hash(5).await.unwrap(), Some(blocks[5].hash()));
        assert_eq!(source.block_hash(6).await.unwrap(), None);
        assert!(matches!(source.block_hash(7).await, Err(HeadError::Remote(_))));
    }

    #[tokio::test]
    async fn a_block_at_another_height_is_refused() {
        let blocks = chain(5);
        let sent = blocks[4].clone();
        let chain = MockChain::new(&blocks);
        let mock = MockNode::start(move |_, request| match request.body["params"][0].as_str() {
            Some("3") => Reply::Result(json!(hex::encode(sent.zcash_serialize_to_vec().unwrap()))),
            _ => chain.reply(request),
        })
        .await;
        let mut source =
            RemoteHeadSource::new(ZakuraClient::new(RpcClient::new(mock.config()).unwrap()));

        let result = source.block(3).await;

        assert!(matches!(result, Err(HeadError::Height { expected: 3, actual: 4 })), "{result:?}");
        assert!(source.block(4).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn sync_head_reaches_a_moving_tip_and_replaces_a_reorged_head() {
        let ctx = EphemeralService::empty().await;
        let mut blocks = chain(20);
        let chain = MockChain::new(&blocks);
        let (_mock, mut source) = node(&chain).await;

        ctx.service.sync_head(&mut source, PipelineConfig::default()).await.unwrap();
        assert_eq!(visible_tip(&ctx.service), Some(id(&blocks[20])));

        grow_chain(&mut blocks, 25, 0);
        chain.set_blocks(&blocks);
        let state = ctx.service.sync_head(&mut source, PipelineConfig::default()).await.unwrap();
        assert_eq!(visible_tip(&ctx.service), Some(id(&blocks[25])));
        assert_eq!(state.durable_tip().unwrap().height, 15);

        // A reorg above the durable tip replaces only the volatile head.
        let mut fork = blocks[..=21].to_vec();
        grow_chain(&mut fork, 26, 1);
        chain.set_blocks(&fork);
        let next = ctx.service.sync_head(&mut source, PipelineConfig::default()).await.unwrap();
        let snapshot = ctx.service.snapshot();
        assert_eq!(snapshot.visible_tip, Some(id(&fork[26])));
        assert_eq!(next.durable_tip().unwrap().height, 16);
        assert!(
            snapshot
                .volatile_head
                .iter()
                .all(|record| record.hash == fork[record.height as usize].hash())
        );
        assert_ne!(fork[22].hash(), blocks[22].hash());
        ctx.index.verify_continuity().unwrap();
    }

    #[tokio::test]
    async fn follow_head_reaches_a_moving_tip() {
        let ctx = EphemeralService::empty().await;
        let mut blocks = chain(12);
        let chain = MockChain::new(&blocks);
        let (_mock, source) = node(&chain).await;
        let (shutdown, receiver) = watch::channel(false);
        let follower = tokio::spawn({
            let service = ctx.service.clone();
            async move {
                service
                    .follow_head(
                        source,
                        PipelineConfig::default(),
                        HeadFollowerConfig {
                            poll_interval: Duration::from_millis(1),
                            attempt_timeout: Duration::from_secs(1),
                            freshness_timeout: Duration::from_secs(1),
                        },
                        receiver,
                    )
                    .await
            }
        });
        let reaches = |tip: BlockId| {
            let service = ctx.service.clone();
            tokio::time::timeout(Duration::from_secs(5), async move {
                while visible_tip(&service) != Some(tip) {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
        };

        reaches(id(&blocks[12])).await.unwrap();
        grow_chain(&mut blocks, 30, 0);
        chain.set_blocks(&blocks);
        reaches(id(&blocks[30])).await.unwrap();

        let mut fork = blocks[..=25].to_vec();
        grow_chain(&mut fork, 31, 1);
        chain.set_blocks(&fork);
        reaches(id(&fork[31])).await.unwrap();
        assert!(ctx.service.readiness().tip);

        shutdown.send(true).unwrap();
        follower.await.unwrap().unwrap();
    }

    /// The requests that `mock` received after the first `from` requests, and how many of them
    /// are `getblock`.
    fn requests_since(mock: &MockNode, from: usize) -> (usize, usize) {
        let seen = &mock.seen()[from..];
        (seen.len(), seen.iter().filter(|request| request.body["method"] == "getblock").count())
    }

    #[tokio::test]
    async fn a_tip_change_costs_requests_for_the_new_blocks_only() {
        let ctx = EphemeralService::empty().await;
        let mut blocks = chain(150);
        let chain = MockChain::new(&blocks);
        let (mock, mut source) = node(&chain).await;
        ctx.service.sync_head(&mut source, PipelineConfig::default()).await.unwrap();

        for (tip, new) in [(151, 1), (154, 3)] {
            grow_chain(&mut blocks, tip, 0);
            chain.set_blocks(&blocks);
            let before = mock.seen().len();
            ctx.service.sync_head(&mut source, PipelineConfig::default()).await.unwrap();

            // The poll sends one `getblockchaininfo` and three `getblockhash`: the anchor, the old
            // tip and the re-check of the new tip. It sends one `getblock` for each new block and
            // one `getblock` for the height above the new tip.
            assert_eq!(requests_since(&mock, before), (5 + new, new + 1));
            assert_eq!(visible_tip(&ctx.service), Some(id(&blocks[tip as usize])));
        }
    }
}
