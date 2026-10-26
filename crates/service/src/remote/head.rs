//! [`CanonicalBlockSource`] over the JSON-RPC interface of a remote zakura node.

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
/// embedded node. Each [`CanonicalBlockSource::block`] call sends two requests: `getblockhash`
/// for the height, then `getblock` with verbosity 0 for that hash. The block bytes go into the
/// index without a second serialization.
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
    /// When the block leaves the best chain between the two requests, the node does not find
    /// its hash and the result is [`HeadError::Changed`].
    async fn block(&mut self, height: u32) -> Result<Option<RawIndexBlock>, HeadError> {
        let hash = match self.node.get_block_hash(block::Height(height)).await {
            Ok(hash) => hash.hash(),
            Err(error) if error.is_block_not_found() => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let bytes = match self.node.get_block_raw(HashOrHeight::Hash(hash)).await {
            Ok(bytes) => bytes.0,
            Err(error) if error.is_block_not_found() => return Err(HeadError::Changed),
            Err(error) => return Err(error.into()),
        };
        let block = RawIndexBlock::from_bytes(bytes)
            .map_err(|error| HeadError::Decode(error.to_string()))?;
        if block.height.0 != height {
            return Err(HeadError::Height { expected: height, actual: block.height.0 });
        }
        if block.hash != hash {
            return Err(HeadError::Hash { height, expected: hash, actual: block.hash });
        }
        Ok(Some(block))
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use serde_json::json;
    use tokio::sync::watch;
    use zakura_chain::{block::Block, parameters::Network, serialization::ZcashSerialize as _};
    use zakura_state::Config;
    use ztreamer_indexer::{
        index::{Index, IndexState},
        pipeline::PipelineConfig,
        test_utils::grow_chain,
    };
    use ztreamer_remote::{
        RpcClient,
        test_utils::{MockChain, MockNode, Reply},
    };

    use super::*;
    use crate::{CompactService, HeadFollowerConfig};

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

    async fn service(dir: &tempfile::TempDir) -> (CompactService, Arc<Index>) {
        let index = Arc::new(
            Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", block::Hash([9; 32])).unwrap(),
        );
        let (_state_service, read_service, _tip, _change) =
            zakura_state::init(Config::ephemeral(), &Network::Mainnet, block::Height::MAX, 0)
                .await
                .expect("ephemeral state initializes");
        (CompactService::new(index.clone(), IndexState::default(), "main", read_service), index)
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
    async fn a_block_with_another_hash_is_refused() {
        let blocks = chain(5);
        let mut fork = blocks[..3].to_vec();
        grow_chain(&mut fork, 3, 1);
        let named = fork[3].hash();
        let sent = blocks[3].clone();
        let chain = MockChain::new(&blocks);
        // The node names the hash of the fork block at height 3, then sends the block of the best
        // chain for that hash.
        let mock = MockNode::start(move |_, request| {
            let params = &request.body["params"];
            match request.body["method"].as_str() {
                Some("getblockhash") if params[0] == 3 => Reply::Result(json!(named.to_string())),
                Some("getblock") if params[0] == named.to_string().as_str() => {
                    Reply::Result(json!(hex::encode(sent.zcash_serialize_to_vec().unwrap())))
                }
                _ => chain.reply(request),
            }
        })
        .await;
        let mut source =
            RemoteHeadSource::new(ZakuraClient::new(RpcClient::new(mock.config()).unwrap()));

        let result = source.block(3).await;

        assert!(
            matches!(
                result,
                Err(HeadError::Hash { height: 3, expected, actual })
                    if expected == fork[3].hash() && actual == blocks[3].hash()
            ),
            "{result:?}"
        );
        assert!(source.block(4).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn sync_head_reaches_a_moving_tip_and_replaces_a_reorged_head() {
        let dir = tempfile::tempdir().unwrap();
        let (service, index) = service(&dir).await;
        let mut blocks = chain(20);
        let chain = MockChain::new(&blocks);
        let (_mock, mut source) = node(&chain).await;

        service.sync_head(&mut source, PipelineConfig::default()).await.unwrap();
        assert_eq!(visible_tip(&service), Some(id(&blocks[20])));

        grow_chain(&mut blocks, 25, 0);
        chain.set_blocks(&blocks);
        let state = service.sync_head(&mut source, PipelineConfig::default()).await.unwrap();
        assert_eq!(visible_tip(&service), Some(id(&blocks[25])));
        assert_eq!(state.durable_tip().unwrap().height, 15);

        // A reorg above the durable tip replaces only the volatile head.
        let mut fork = blocks[..=21].to_vec();
        grow_chain(&mut fork, 26, 1);
        chain.set_blocks(&fork);
        let next = service.sync_head(&mut source, PipelineConfig::default()).await.unwrap();
        let snapshot = service.snapshot();
        assert_eq!(snapshot.visible_tip, Some(id(&fork[26])));
        assert_eq!(next.durable_tip().unwrap().height, 16);
        assert!(
            snapshot
                .volatile_head
                .iter()
                .all(|record| record.hash == fork[record.height as usize].hash())
        );
        assert_ne!(fork[22].hash(), blocks[22].hash());
        index.verify_continuity().unwrap();
    }

    #[tokio::test]
    async fn follow_head_reaches_a_moving_tip() {
        let dir = tempfile::tempdir().unwrap();
        let (service, _index) = service(&dir).await;
        let mut blocks = chain(12);
        let chain = MockChain::new(&blocks);
        let (_mock, source) = node(&chain).await;
        let (shutdown, receiver) = watch::channel(false);
        let follower = tokio::spawn({
            let service = service.clone();
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
            let service = service.clone();
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
        assert!(service.readiness().tip);

        shutdown.send(true).unwrap();
        follower.await.unwrap().unwrap();
    }
}
