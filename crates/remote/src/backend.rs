//! A [`StateBackend`] over the JSON-RPC of a remote zakura node.
//!
//! Each method calls the node through [`ZakuraClient`] and parses the answer into the
//! `zakura_chain` type of the trait. A parse failure is [`BackendError::Unexpected`].
//!
//! The backend keeps two caches, and a cache hit makes no call to the node. The tree state of a
//! block hash does not change, and that cache holds at most 4,096 of them. The mempool cache
//! holds the transactions of the latest `getrawmempool` answer, so the node mempool bounds it.

use std::{
    collections::{HashMap, HashSet},
    num::NonZeroUsize,
    ops::RangeInclusive,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use crate::{RpcError, ZakuraClient, frontier::FromRpc};
use futures_util::{StreamExt as _, TryStreamExt as _, future, stream};
use lru::LruCache;
use sapling_crypto::Node as SaplingNode;
use zakura_chain::{
    amount::{Amount, NonNegative},
    block::{self, Block},
    ironwood, orchard,
    parameters::Network,
    sapling,
    serialization::{ZcashDeserialize, ZcashDeserializeInto as _, ZcashSerialize as _},
    subtree::{NoteCommitmentSubtreeData, NoteCommitmentSubtreeIndex},
    transaction::{self, Transaction, UnminedTx},
    transparent,
};
use zakura_rpc::{
    client::{
        GetAddressBalanceRequest, GetAddressTxIdsRequest, GetAddressUtxosResponse,
        GetRawMempoolResponse, GetRawTransactionResponse, GetSubtreesByIndexResponse,
        TransactionObject, Treestate,
    },
    methods::GetAddressUtxosRequest,
};
use zakura_state::HashOrHeight;
use ztreamer_snapshot::pool::{self, NodeBytes, SubtreeNode};

use ztreamer_backend::{
    AddressUtxo, BackendError, MinedTransaction, StateBackend, Subtrees, TreeState,
};

/// The tree states that the cache can hold. A tree state is at most three frontiers of 32
/// nodes, approximately 3.5 KiB, so the full cache is approximately 14 MiB.
const TREE_CACHE_ENTRIES: NonZeroUsize = NonZeroUsize::new(4_096).unwrap();

/// The most `getrawtransaction` calls in flight for one mempool read.
const MEMPOOL_FETCHES: usize = 16;

/// Why [`RemoteBackend::connect`] refused the node.
#[derive(Debug, thiserror::Error)]
pub enum RemoteConnectError {
    /// The node did not answer.
    #[error(transparent)]
    Rpc(#[from] RpcError),
    /// The node runs a different chain.
    #[error("remote zakura runs chain {found:?}, the index needs {expected:?}")]
    WrongChain {
        /// The BIP70 name of the index's network.
        expected: String,
        /// The chain name that the node gave.
        found: String,
    },
    /// The node has a different genesis block.
    #[error("remote zakura has genesis block {found}, the index needs {expected}")]
    WrongGenesis {
        /// The genesis hash of the index's network.
        expected: block::Hash,
        /// The genesis hash that the node gave.
        found: block::Hash,
    },
}

/// A failed call to the remote node. It converts to the [`BackendError`] that the call gives.
struct NodeCallError(RpcError);

impl From<NodeCallError> for BackendError {
    fn from(NodeCallError(error): NodeCallError) -> Self {
        match error {
            RpcError::Protocol { .. } | RpcError::Decode { .. } => {
                Self::Unexpected(format!("remote zakura: {error}"))
            }
            _ => Self::Unavailable(format!("remote zakura: {error}")),
        }
    }
}

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
        let response = match self.node.get_raw_transaction(id).await {
            Ok(response) => response,
            Err(error) if error.is_transaction_not_found() => return Ok(None),
            Err(error) => return Err(NodeCallError(error).into()),
        };
        let GetRawTransactionResponse::Object(object) = response else {
            return Err(BackendError::Unexpected(
                "remote zakura sent a raw transaction for a verbose request".into(),
            ));
        };
        Ok(Some(FetchedTransaction { id, object }))
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

/// A verbose `getrawtransaction` answer for one transaction id.
struct FetchedTransaction {
    id: transaction::Hash,
    object: Box<TransactionObject>,
}

impl FetchedTransaction {
    /// The best-chain height of the transaction. `None` for a mempool transaction. A height of
    /// -1 is a side chain, which is not the best chain either.
    fn height(&self) -> Option<block::Height> {
        self.object.height().and_then(|height| u32::try_from(height).ok()).map(block::Height)
    }

    /// Decodes the transaction. Its id must be the id of the request.
    async fn decode(self) -> Result<Transaction, BackendError> {
        let transaction = Transaction::from_node_bytes(self.object.hex().as_ref().to_vec()).await?;
        if transaction.hash() != self.id {
            return Err(BackendError::Unexpected(format!(
                "remote zakura sent transaction {} for {}",
                transaction.hash(),
                self.id
            )));
        }
        Ok(transaction)
    }
}

/// A value that the node sends in the Zcash serialization: a block or a transaction.
trait FromNodeBytes: ZcashDeserialize + Send + 'static {
    /// The name of the value in errors.
    const WHAT: &'static str;

    /// Deserializes `bytes` on a blocking thread, because a large block takes milliseconds. The
    /// thread runs in the tracing span of the caller.
    fn from_node_bytes(bytes: Vec<u8>) -> impl Future<Output = Result<Self, BackendError>> + Send {
        let span = tracing::Span::current();
        async move {
            tokio::task::spawn_blocking(move || {
                span.in_scope(|| bytes.zcash_deserialize_into::<Self>())
            })
            .await
            .map_err(|error| BackendError::Unavailable(format!("decode task failed: {error}")))?
            .map_err(|error| {
                BackendError::Unexpected(format!(
                    "remote zakura sent a bad {}: {error}",
                    Self::WHAT
                ))
            })
        }
    }
}

impl FromNodeBytes for Block {
    const WHAT: &'static str = "block";
}

impl FromNodeBytes for Transaction {
    const WHAT: &'static str = "transaction";
}

/// A note commitment tree that `z_gettreestate` prints, and the type of its root.
trait RpcTree: FromRpc {
    type Root: FromRpc + PartialEq;

    fn tree_root(&self) -> Self::Root;

    /// Reads the tree from `finalState` of one pool of a `z_gettreestate` answer. `None` when
    /// the node leaves it out, which it does before the pool activates. When the node also gives
    /// `finalRoot`, the root of the tree must be equal to it.
    fn from_treestate(
        pool: &str,
        treestate: &Treestate,
    ) -> Result<Option<Arc<Self>>, BackendError> {
        let commitments = treestate.commitments();
        let Some(state) = commitments.final_state() else {
            return Ok(None);
        };
        let bad = |error| {
            BackendError::Unexpected(format!("remote zakura sent a bad {pool} tree: {error}"))
        };
        let tree = Self::from_rpc_bytes(state).map_err(bad)?;
        let root = commitments.final_root().as_deref().map(Self::Root::from_rpc_bytes).transpose();
        if root.map_err(bad)?.is_some_and(|root| root != tree.tree_root()) {
            return Err(BackendError::Unexpected(format!(
                "remote zakura sent a {pool} tree that does not match its root"
            )));
        }
        Ok(Some(Arc::new(tree)))
    }
}

impl RpcTree for sapling::tree::NoteCommitmentTree {
    type Root = sapling::tree::Root;

    fn tree_root(&self) -> Self::Root {
        self.root()
    }
}

impl RpcTree for orchard::tree::NoteCommitmentTree {
    type Root = orchard::tree::Root;

    fn tree_root(&self) -> Self::Root {
        self.root()
    }
}

/// A shielded pool: its [`pool::Pool`] and its root type.
trait Pool {
    const POOL: pool::Pool;
    /// The name of the pool in `z_getsubtreesbyindex`.
    const NAME: &'static str = Self::POOL.as_str();
    type Node: Clone + Send + Sync + 'static;

    /// Returns the root in `node`. Returns `None` if `node` is a root of a different pool.
    fn root_from_node(node: SubtreeNode) -> Option<Self::Node>;

    /// Parses a `z_getsubtreesbyindex` answer that must start at `start`.
    ///
    /// The node prints each subtree root in the stored byte order, for all pools. This is
    /// different from the Sapling tree root (`finalRoot` of `z_gettreestate`), which the node
    /// prints reversed.
    fn parse_subtrees(
        response: GetSubtreesByIndexResponse,
        start: NoteCommitmentSubtreeIndex,
    ) -> Result<Subtrees<Self::Node>, BackendError> {
        if response.pool() != Self::NAME || response.start_index() != start {
            return Err(BackendError::Unexpected(format!(
                "remote zakura answered {} subtrees from {}, not {} from {}",
                response.pool(),
                response.start_index().0,
                Self::NAME,
                start.0,
            )));
        }
        let bad_root = |root: &str| {
            BackendError::Unexpected(format!(
                "remote zakura sent a bad {} subtree root {root:?}",
                Self::NAME
            ))
        };
        response
            .subtrees()
            .iter()
            .zip(u32::from(start.0)..)
            .map(|(subtree, index)| {
                let index = u16::try_from(index).map_err(|_| {
                    BackendError::Unexpected(format!(
                        "remote zakura sent {} subtrees past index 65535",
                        Self::NAME
                    ))
                })?;
                let root = subtree
                    .root
                    .parse::<NodeBytes>()
                    .ok()
                    .and_then(|bytes| Self::POOL.node(bytes))
                    .and_then(Self::root_from_node)
                    .ok_or_else(|| bad_root(&subtree.root))?;
                let data = NoteCommitmentSubtreeData::new(subtree.end_height, root);
                Ok((NoteCommitmentSubtreeIndex(index), data))
            })
            .collect()
    }
}

enum SaplingPool {}
enum OrchardPool {}
enum IronwoodPool {}

impl Pool for SaplingPool {
    const POOL: pool::Pool = pool::Pool::Sapling;
    type Node = SaplingNode;

    fn root_from_node(node: SubtreeNode) -> Option<Self::Node> {
        match node {
            SubtreeNode::Sapling(root) => Some(root),
            _ => None,
        }
    }
}

impl Pool for OrchardPool {
    const POOL: pool::Pool = pool::Pool::Orchard;
    type Node = orchard::tree::Node;

    fn root_from_node(node: SubtreeNode) -> Option<Self::Node> {
        match node {
            SubtreeNode::Orchard(root) => Some(root),
            _ => None,
        }
    }
}

impl Pool for IronwoodPool {
    const POOL: pool::Pool = pool::Pool::Ironwood;
    type Node = ironwood::tree::Node;

    fn root_from_node(node: SubtreeNode) -> Option<Self::Node> {
        match node {
            SubtreeNode::Ironwood(root) => Some(root),
            _ => None,
        }
    }
}

#[tonic::async_trait]
impl StateBackend for RemoteBackend {
    fn network(&self) -> Network {
        self.network.clone()
    }

    async fn tip_height(&self) -> Result<Option<block::Height>, BackendError> {
        Ok(Some(self.node.get_block_count().await.map_err(NodeCallError)?))
    }

    async fn block(&self, hash: block::Hash) -> Result<Option<Arc<Block>>, BackendError> {
        let raw = match self.node.get_block_raw(HashOrHeight::Hash(hash)).await {
            Ok(raw) => raw,
            Err(error) if error.is_block_not_found() => return Ok(None),
            Err(error) => return Err(NodeCallError(error).into()),
        };
        let block = Block::from_node_bytes(raw.0).await?;
        if block.hash() != hash {
            return Err(BackendError::Unexpected(format!(
                "remote zakura sent block {} for {hash}",
                block.hash()
            )));
        }
        Ok(Some(Arc::new(block)))
    }

    async fn transaction(
        &self,
        hash: transaction::Hash,
    ) -> Result<Option<MinedTransaction>, BackendError> {
        let Some(fetched) = self.fetch_transaction(hash).await? else {
            return Ok(None);
        };
        let Some(height) = fetched.height() else {
            return Ok(None);
        };
        let transaction = Arc::new(fetched.decode().await?);
        Ok(Some(MinedTransaction { transaction, height }))
    }

    async fn tree_state(&self, hash: block::Hash) -> Result<TreeState, BackendError> {
        let cached = self.lock_tree_states().get(&hash).cloned();
        if let Some(trees) = cached {
            return Ok(trees);
        }
        let response =
            self.node.z_get_treestate(HashOrHeight::Hash(hash)).await.map_err(NodeCallError)?;
        if response.hash() != hash {
            return Err(BackendError::Unexpected(format!(
                "remote zakura sent the tree state of {} for {hash}",
                response.hash()
            )));
        }
        let span = tracing::Span::current();
        let trees = tokio::task::spawn_blocking(move || {
            span.in_scope(|| {
                Ok::<_, BackendError>(TreeState {
                    sapling: RpcTree::from_treestate("Sapling", response.sapling())?,
                    orchard: RpcTree::from_treestate("Orchard", response.orchard())?,
                    ironwood: RpcTree::from_treestate("Ironwood", response.ironwood())?,
                })
            })
        })
        .await
        .map_err(|error| BackendError::Unavailable(format!("decode task failed: {error}")))??;
        self.lock_tree_states().put(hash, trees.clone());
        Ok(trees)
    }

    async fn sapling_subtrees(
        &self,
        start: NoteCommitmentSubtreeIndex,
        limit: Option<NoteCommitmentSubtreeIndex>,
    ) -> Result<Subtrees<SaplingNode>, BackendError> {
        self.subtrees::<SaplingPool>(start, limit).await
    }

    async fn orchard_subtrees(
        &self,
        start: NoteCommitmentSubtreeIndex,
        limit: Option<NoteCommitmentSubtreeIndex>,
    ) -> Result<Subtrees<orchard::tree::Node>, BackendError> {
        self.subtrees::<OrchardPool>(start, limit).await
    }

    async fn ironwood_subtrees(
        &self,
        start: NoteCommitmentSubtreeIndex,
        limit: Option<NoteCommitmentSubtreeIndex>,
    ) -> Result<Subtrees<ironwood::tree::Node>, BackendError> {
        self.subtrees::<IronwoodPool>(start, limit).await
    }

    async fn address_transaction_ids(
        &self,
        address: transparent::Address,
        heights: RangeInclusive<block::Height>,
    ) -> Result<Vec<transaction::Hash>, BackendError> {
        // The node reads an end of 0 as the tip. The genesis block is not in the address index,
        // so a range that ends at it has no transactions.
        if heights.end().0 == 0 {
            return Ok(Vec::new());
        }
        // The node also moves a start above its tip down to the tip. Read the tip first: the tip
        // of the node does not go down, so the node then keeps the start as it is. A start of 0
        // cannot be above the tip.
        if heights.start().0 > 0
            && *heights.start() > self.node.get_block_count().await.map_err(NodeCallError)?
        {
            return Ok(Vec::new());
        }
        let request = GetAddressTxIdsRequest::new(
            vec![address.to_string()],
            Some(heights.start().0),
            Some(heights.end().0),
        );
        self.node
            .get_address_tx_ids(&request)
            .await
            .map_err(NodeCallError)?
            .iter()
            .map(|id| {
                id.parse().map_err(|_| {
                    BackendError::Unexpected(format!("remote zakura sent a bad txid {id:?}"))
                })
            })
            .collect()
    }

    async fn address_balance(
        &self,
        addresses: HashSet<transparent::Address>,
    ) -> Result<Amount<NonNegative>, BackendError> {
        let request =
            GetAddressBalanceRequest::new(addresses.iter().map(ToString::to_string).collect());
        Ok(Amount::try_from(
            self.node.get_address_balance(&request).await.map_err(NodeCallError)?.balance(),
        )?)
    }

    async fn address_utxos(
        &self,
        addresses: HashSet<transparent::Address>,
    ) -> Result<Vec<AddressUtxo>, BackendError> {
        let addresses = addresses.iter().map(ToString::to_string).collect();
        let request = GetAddressUtxosRequest::new(addresses, false);
        let utxos = match self.node.get_address_utxos(&request).await.map_err(NodeCallError)? {
            GetAddressUtxosResponse::Utxos(utxos) => utxos,
            GetAddressUtxosResponse::UtxosAndChainInfo(object) => object.utxos().clone(),
        };
        utxos
            .iter()
            .map(|utxo| {
                let (address, hash, index, lock_script, zatoshis, height) = utxo.into_parts();
                Ok(AddressUtxo {
                    address,
                    outpoint: transparent::OutPoint { hash, index: index.index() },
                    output: transparent::Output { value: Amount::try_from(zatoshis)?, lock_script },
                    height,
                })
            })
            .collect()
    }

    async fn mempool_transactions(&self) -> Result<Vec<UnminedTx>, BackendError> {
        let ids = match self.node.get_raw_mempool().await.map_err(NodeCallError)? {
            GetRawMempoolResponse::TxIds(ids) => ids,
            GetRawMempoolResponse::Verbose(objects) => objects.into_keys().collect(),
        };
        let ids: Vec<transaction::Hash> = ids
            .iter()
            .map(|id| {
                id.parse().map_err(|_| {
                    BackendError::Unexpected(format!("remote zakura sent a bad mempool id {id:?}"))
                })
            })
            .collect::<Result<_, _>>()?;
        // The cache keeps only the transactions that the node still has in its mempool.
        let listed: HashSet<transaction::Hash> = ids.iter().copied().collect();
        self.lock_mempool().retain(|id, _| listed.contains(id));
        stream::iter(ids)
            .map(|id| async move {
                let fetched = self.mempool_transaction(id).await;
                (id, fetched)
            })
            .buffer_unordered(MEMPOOL_FETCHES)
            // A failed call for one transaction does not fail the read. That transaction is not
            // in the cache, so the next read asks for it again.
            .filter_map(|(id, fetched)| {
                future::ready(match fetched {
                    Err(BackendError::Unavailable(error)) => {
                        tracing::warn!(txid = %id, %error, "remote zakura: skipped a mempool transaction");
                        None
                    }
                    fetched => fetched.transpose(),
                })
            })
            .try_collect()
            .await
    }

    async fn submit_transaction(
        &self,
        transaction: Transaction,
    ) -> Result<transaction::Hash, BackendError> {
        let bytes = transaction.zcash_serialize_to_vec().map_err(|error| {
            BackendError::Rejected(format!("transaction does not serialize: {error}"))
        })?;
        match self.node.send_raw_transaction(&bytes).await {
            Ok(sent) => Ok(sent.hash()),
            // Warm-up and a method that the listener does not serve are about the node, not
            // about the transaction.
            Err(error) if error.is_retryable() || error.is_method_not_found() => {
                Err(NodeCallError(error).into())
            }
            Err(RpcError::Rpc { code, message, .. }) => Err(BackendError::Rejected(format!(
                "remote zakura rejected transaction: {message} ({code})"
            ))),
            Err(error) => Err(NodeCallError(error).into()),
        }
    }
}

#[cfg(test)]
mod tests;
