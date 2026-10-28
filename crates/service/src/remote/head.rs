//! [`CanonicalBlockSource`] over the JSON-RPC interface of a remote zakura node.

use zakura_chain::block;
use ztreamer_indexer::{
    head::{CanonicalBlockSource, HeadError},
    index::BlockId,
    types::{BlockAtHeightError, FetchAtHeightError, RawIndexBlock},
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
        match self.node.block_at_height(height).await {
            Ok(block) => Ok(Some(block)),
            Err(FetchAtHeightError::Block(BlockAtHeightError::Rpc(error)))
                if error.is_block_not_found() =>
            {
                Ok(None)
            }
            Err(error) => Err(error.into()),
        }
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
mod tests;
