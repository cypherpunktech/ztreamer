//! A [`StateBackend`] over the JSON-RPC of a remote zakura node.
//!
//! Each method calls the node through [`ZakuraClient`] and parses the answer into the
//! `zakura_chain` type of the trait. A parse failure is [`BackendError::Unexpected`].
//!
//! The backend keeps two caches, and a cache hit makes no call to the node. The tree state of a
//! block hash does not change, and that cache holds at most 4,096 of them. The mempool cache
//! holds the transactions of the latest `getrawmempool` answer, so the node mempool bounds it.
//!
//! [`StateBackend`]: ztreamer_backend::StateBackend

mod error;
mod fetched_transaction;
mod node_bytes;
mod rpc_tree;
mod shielded_pool;
mod state_backend;

use std::{
    collections::HashMap,
    num::NonZeroUsize,
    sync::{Mutex, MutexGuard, PoisonError},
};

use crate::ZakuraClient;
use lru::LruCache;
use zakura_chain::{
    block,
    parameters::Network,
    subtree::NoteCommitmentSubtreeIndex,
    transaction::{self, UnminedTx},
};
use ztreamer_backend::{BackendError, Subtrees, TreeState};

use error::NodeCallError;
use fetched_transaction::FetchedTransaction;
use shielded_pool::Pool;

pub use error::RemoteConnectError;

/// The tree states that the cache can hold. A tree state is at most three frontiers of 32
/// nodes, approximately 3.5 KiB, so the full cache is approximately 14 MiB.
const TREE_CACHE_ENTRIES: NonZeroUsize = NonZeroUsize::new(4_096).unwrap();

/// Chain state from a remote zakura node.
///
/// ```no_run
/// use zakura_chain::parameters::Network;
/// use ztreamer_remote::{RemoteBackend, RpcClient, ZakuraClient};
///
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// let config = ZakuraClient::config("http://127.0.0.1:8232".parse()?);
/// let node = ZakuraClient::new(RpcClient::new(config)?);
/// let backend = RemoteBackend::connect(node, Network::Mainnet).await?;
/// # let _ = backend;
/// # Ok(())
/// # }
/// ```
pub struct RemoteBackend {
    node: ZakuraClient,
    network: Network,
    /// The tree states that the node gave. They do not change.
    tree_states: Mutex<LruCache<block::Hash, TreeState>>,
    /// The transactions of the latest `getrawmempool` answer. The node mempool bounds it.
    mempool: Mutex<HashMap<transaction::Hash, UnminedTx>>,
}

impl std::fmt::Debug for RemoteBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteBackend")
            .field("node", &self.node)
            .field("network", &self.network)
            .finish_non_exhaustive()
    }
}

impl RemoteBackend {
    /// Connects to the node and makes sure that it runs `network`: the chain name and the
    /// genesis hash must match.
    pub async fn connect(node: ZakuraClient, network: Network) -> Result<Self, RemoteConnectError> {
        let (info, genesis) =
            tokio::try_join!(node.get_blockchain_info(), node.get_block_hash(block::Height(0)))?;
        let expected = network.bip70_network_name();
        if *info.chain() != expected {
            return Err(RemoteConnectError::WrongChain { expected, found: info.chain().clone() });
        }
        let (expected, found) = (network.genesis_hash(), genesis.hash());
        if found != expected {
            return Err(RemoteConnectError::WrongGenesis { expected, found });
        }
        Ok(Self {
            node,
            network,
            tree_states: Mutex::new(LruCache::new(TREE_CACHE_ENTRIES)),
            mempool: Mutex::default(),
        })
    }

    /// Locks the tree-state cache. A panic in another holder leaves the cache valid.
    fn lock_tree_states(&self) -> MutexGuard<'_, LruCache<block::Hash, TreeState>> {
        self.tree_states.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Locks the mempool cache. A panic in another holder leaves the cache valid.
    fn lock_mempool(&self) -> MutexGuard<'_, HashMap<transaction::Hash, UnminedTx>> {
        self.mempool.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Completed subtrees of `P` from `start`, at most `limit` of them.
    async fn subtrees<P: Pool>(
        &self,
        start: NoteCommitmentSubtreeIndex,
        limit: Option<NoteCommitmentSubtreeIndex>,
    ) -> Result<Subtrees<P::Node>, BackendError> {
        P::parse_subtrees(
            self.node
                .z_get_subtrees_by_index(P::NAME, start, limit)
                .await
                .map_err(NodeCallError)?,
            start,
        )
    }

    /// The `getrawtransaction` answer for `id` with verbose 1. `None` when the node does not
    /// have the transaction.
    async fn fetch_transaction(
        &self,
        id: transaction::Hash,
    ) -> Result<Option<FetchedTransaction>, BackendError> {
        match self.node.get_raw_transaction(id).await {
            Ok(response) => FetchedTransaction::from_response(id, response).map(Some),
            Err(error) if error.is_transaction_not_found() => Ok(None),
            Err(error) => Err(NodeCallError(error).into()),
        }
    }

    /// A mempool transaction, from the cache or the node. `None` when the transaction left the
    /// mempool or was mined after `getrawmempool`.
    ///
    /// The cache key is the mined id, so the cache keeps the first authorizing data that the
    /// node gave for a version 5 transaction.
    async fn mempool_transaction(
        &self,
        id: transaction::Hash,
    ) -> Result<Option<UnminedTx>, BackendError> {
        let cached = self.lock_mempool().get(&id).cloned();
        if cached.is_some() {
            return Ok(cached);
        }
        let Some(fetched) = self.fetch_transaction(id).await? else {
            return Ok(None);
        };
        if fetched.height().is_some() {
            return Ok(None);
        }
        let unmined = UnminedTx::from(fetched.decode().await?);
        self.lock_mempool().insert(id, unmined.clone());
        Ok(Some(unmined))
    }
}

#[cfg(test)]
mod tests;
