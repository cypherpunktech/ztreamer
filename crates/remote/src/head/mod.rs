//! [`CanonicalBlockSource`] over the JSON-RPC interface of a remote zakura node.

use std::time::Duration;

use crate::{FetchAtHeightError, NodeState, RpcError, ZakuraClient};
use zakura_chain::block;
use ztreamer_chain::types::RawIndexBlock;
use ztreamer_index::index::BlockId;
use ztreamer_sync::head::{CanonicalBlockSource, HeadError};

/// The longest time of one call, with the retries of the client. It is below the 30 s attempt
/// timeout of the follower, thus a node that does not answer gives [`RpcError::Timeout`] and
/// the follower stops.
const CALL_TIMEOUT: Duration = Duration::from_secs(20);

/// The canonical head of a remote zakura node, read over JSON-RPC.
///
/// [`HeadFollower::sync_head`](ztreamer_sync::head::HeadFollower::sync_head) and
/// [`HeadFollower::follow_head`](ztreamer_sync::head::HeadFollower::follow_head) use it as
/// they use the embedded node. Each [`CanonicalBlockSource::block_hash`] call sends one `getblockhash`
/// request. Each [`CanonicalBlockSource::block`] call sends one `getblock` request with the
/// height and verbosity 0. The block bytes go into the index without a second serialization.
///
/// ```no_run
/// use ztreamer_remote::{RemoteHeadSource, RpcClient, ZakuraClient};
///
/// # fn demo() -> Result<(), ztreamer_remote::RpcError> {
/// let config = ZakuraClient::config("http://127.0.0.1:8232".parse().unwrap());
/// let source = RemoteHeadSource::new(ZakuraClient::new(RpcClient::new(config)?));
/// # let _ = source;
/// # Ok(())
/// # }
/// ```
///
/// A failed call is [`HeadError::RemoteFatal`] when the node is gone or refuses the call
/// ([`RpcError::node_state`]), and the follower stops. It is [`HeadError::Remote`] when the node
/// is syncing, and the follower tries again. A call that does not end in 20 s is
/// [`RpcError::Timeout`]: the node is gone.
#[derive(Clone, Debug)]
pub struct RemoteHeadSource {
    node: ZakuraClient,
    call_timeout: Duration,
}

impl RemoteHeadSource {
    /// A source that reads the head of `node`.
    pub const fn new(node: ZakuraClient) -> Self {
        Self { node, call_timeout: CALL_TIMEOUT }
    }

    /// The source with another longest time of one call.
    #[cfg(test)]
    const fn with_call_timeout(mut self, call_timeout: Duration) -> Self {
        self.call_timeout = call_timeout;
        self
    }

    /// The answer of `call`, a call of `method`. A call that does not end in the call timeout
    /// fails with [`RpcError::Timeout`].
    async fn answer_in_time<T, E>(
        &self,
        method: &str,
        call: impl Future<Output = Result<T, E>>,
    ) -> Result<Result<T, E>, HeadError> {
        let timeout = self.call_timeout;
        tokio::time::timeout(timeout, call)
            .await
            .map_err(|_| self.call_failed(RpcError::Timeout { method: method.to_owned(), timeout }))
    }

    /// The [`HeadError`] of a failed call, with the URL of the node.
    fn call_failed(&self, source: RpcError) -> HeadError {
        let state = source.node_state();
        let error =
            Box::new(HeadCallError { url: self.node.rpc().url().to_string(), state, source });
        match state {
            NodeState::Syncing => HeadError::Remote(error),
            NodeState::Gone | NodeState::Refuses => HeadError::RemoteFatal(error),
        }
    }
}

/// A failed call of a [`RemoteHeadSource`], with the URL of the node and the state that the
/// error shows.
#[derive(Debug, thiserror::Error)]
#[error("remote zakura {url} {state}: {source}")]
struct HeadCallError {
    url: String,
    state: NodeState,
    #[source]
    source: RpcError,
}

#[tonic::async_trait]
impl CanonicalBlockSource for RemoteHeadSource {
    /// The best block, from one `getblockchaininfo` call. The node takes the height and the hash
    /// from the same chain tip, thus they agree.
    async fn tip(&mut self) -> Result<Option<BlockId>, HeadError> {
        let info = self.answer_in_time("getblockchaininfo", self.node.get_blockchain_info());
        let info = info.await?.map_err(|error| self.call_failed(error))?;
        Ok(Some(BlockId::new(info.blocks().0, info.best_block_hash())))
    }

    /// The best-chain block at `height`, or `None` when the node has no block at that height.
    ///
    /// It decodes the block on the blocking thread pool. The hash comes from the block. [`poll_canonical_head`] checks that each block links to
    /// the block below it. After the reads, it reads the hash of the last block again.
    ///
    /// [`poll_canonical_head`]: ztreamer_sync::head::poll_canonical_head
    async fn block(&mut self, height: u32) -> Result<Option<RawIndexBlock>, HeadError> {
        match self.answer_in_time("getblock", self.node.block_at_height(height)).await? {
            Ok(block) => Ok(Some(block)),
            Err(FetchAtHeightError::Rpc(error)) if error.is_block_not_found() => Ok(None),
            Err(FetchAtHeightError::Rpc(error)) => Err(self.call_failed(error)),
            Err(error) => Err(error.into()),
        }
    }

    /// The best-chain hash at `height` from one `getblockhash` call, or `None` when the node has
    /// no block at that height.
    async fn block_hash(&mut self, height: u32) -> Result<Option<block::Hash>, HeadError> {
        let hash = self.node.get_block_hash(block::Height(height));
        match self.answer_in_time("getblockhash", hash).await? {
            Ok(hash) => Ok(Some(hash.hash())),
            Err(error) if error.is_block_not_found() => Ok(None),
            Err(error) => Err(self.call_failed(error)),
        }
    }
}

#[cfg(test)]
mod tests;
