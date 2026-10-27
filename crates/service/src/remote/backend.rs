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
use ztreamer_indexer::{
    frontier::FromRpc,
    index::range_meta::{self, NodeBytes, SubtreeNode},
};
use ztreamer_remote::{FirstBlockWithBody as _, RpcError, ZakuraClient};

use crate::backend::{
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

impl From<RpcError> for BackendError {
    fn from(error: RpcError) -> Self {
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
/// use ztreamer_remote::{RpcClient, ZakuraClient};
/// use ztreamer_service::remote::RemoteBackend;
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
        P::parse_subtrees(self.node.z_get_subtrees_by_index(P::NAME, start, limit).await?, start)
    }

    /// The error for a `z_gettreestate` that did not find the block of `hash`.
    ///
    /// `z_gettreestate` of zakura reads the block body, also when the node keeps the trees. A
    /// node that prunes thus has no tree state below its prune height. The error says so.
    async fn no_tree_state(&self, hash: block::Hash, error: RpcError) -> BackendError {
        match self.node.get_blockchain_info().await.map(|info| info.first_block_with_body()) {
            Ok(first) if first > block::Height::MIN => BackendError::Unavailable(format!(
                "remote zakura has no tree state for {hash}: the block is not in its best chain, \
                 or it is below height {}, and the node prunes the block bodies below that \
                 height. z_gettreestate reads the block body",
                first.0
            )),
            _ => error.into(),
        }
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
            Err(error) => return Err(error.into()),
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

/// A shielded pool: its [`range_meta::Pool`] and its root type.
trait Pool {
    const POOL: range_meta::Pool;
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
    const POOL: range_meta::Pool = range_meta::Pool::Sapling;
    type Node = SaplingNode;

    fn root_from_node(node: SubtreeNode) -> Option<Self::Node> {
        match node {
            SubtreeNode::Sapling(root) => Some(root),
            _ => None,
        }
    }
}

impl Pool for OrchardPool {
    const POOL: range_meta::Pool = range_meta::Pool::Orchard;
    type Node = orchard::tree::Node;

    fn root_from_node(node: SubtreeNode) -> Option<Self::Node> {
        match node {
            SubtreeNode::Orchard(root) => Some(root),
            _ => None,
        }
    }
}

impl Pool for IronwoodPool {
    const POOL: range_meta::Pool = range_meta::Pool::Ironwood;
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
        Ok(Some(self.node.get_block_count().await?))
    }

    async fn block(&self, hash: block::Hash) -> Result<Option<Arc<Block>>, BackendError> {
        let raw = match self.node.get_block_raw(HashOrHeight::Hash(hash)).await {
            Ok(raw) => raw,
            Err(error) if error.is_block_not_found() => return Ok(None),
            Err(error) => return Err(error.into()),
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
        let response = match self.node.z_get_treestate(HashOrHeight::Hash(hash)).await {
            Ok(response) => response,
            Err(error) if error.is_block_not_found() => {
                return Err(self.no_tree_state(hash, error).await);
            }
            Err(error) => return Err(error.into()),
        };
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
        if heights.start().0 > 0 && *heights.start() > self.node.get_block_count().await? {
            return Ok(Vec::new());
        }
        let request = GetAddressTxIdsRequest::new(
            vec![address.to_string()],
            Some(heights.start().0),
            Some(heights.end().0),
        );
        self.node
            .get_address_tx_ids(&request)
            .await?
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
        Ok(Amount::try_from(self.node.get_address_balance(&request).await?.balance())?)
    }

    async fn address_utxos(
        &self,
        addresses: HashSet<transparent::Address>,
    ) -> Result<Vec<AddressUtxo>, BackendError> {
        let addresses = addresses.iter().map(ToString::to_string).collect();
        let request = GetAddressUtxosRequest::new(addresses, false);
        let utxos = match self.node.get_address_utxos(&request).await? {
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
        let ids = match self.node.get_raw_mempool().await? {
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
            Err(error) if error.is_retryable() || error.is_method_not_found() => Err(error.into()),
            Err(RpcError::Rpc { code, message, .. }) => Err(BackendError::Rejected(format!(
                "remote zakura rejected transaction: {message} ({code})"
            ))),
            Err(error) => Err(error.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::atomic::{AtomicBool, Ordering},
    };

    use super::*;
    use serde_json::{Value, json};
    use zakura_rpc::client::{
        Commitments, GetBlockchainInfoResponse, GetTreestateResponse, HexData, TransactionObject,
        Utxo,
    };
    use zakura_test::vectors::{BLOCK_MAINNET_1_BYTES, BLOCK_MAINNET_2_BYTES};
    use ztreamer_remote::{
        RpcClient, RpcCode,
        test_utils::{MockNode, Reply},
    };

    /// The mainnet answers that `connect` and the tip reads need, for a node with tip `tip`.
    fn chain_reply(method: &str, tip: u32) -> Option<Reply> {
        Some(match method {
            "getblockchaininfo" => {
                let mut info = serde_json::to_value(GetBlockchainInfoResponse::default()).unwrap();
                info["blocks"] = json!(tip);
                Reply::Result(info)
            }
            "getblockcount" => Reply::Result(json!(tip)),
            "getblockhash" => Reply::Result(json!(Network::Mainnet.genesis_hash().to_string())),
            _ => return None,
        })
    }

    /// A mock mainnet node with tip `tip`. It answers other methods from `answer`.
    async fn mainnet(
        tip: u32,
        answer: impl Fn(&str, &Value) -> Reply + Send + Sync + 'static,
    ) -> MockNode {
        MockNode::start(move |_, seen| {
            let (method, params) = (seen.body["method"].as_str().unwrap(), &seen.body["params"]);
            chain_reply(method, tip).unwrap_or_else(|| answer(method, params))
        })
        .await
    }

    async fn backend(mock: &MockNode) -> RemoteBackend {
        let node = ZakuraClient::new(RpcClient::new(mock.config()).unwrap());
        RemoteBackend::connect(node, Network::Mainnet).await.unwrap()
    }

    fn block_1() -> Block {
        BLOCK_MAINNET_1_BYTES.zcash_deserialize_into().unwrap()
    }

    fn orchard_root(seed: u8) -> orchard::tree::Node {
        orchard::tree::Node::try_from([seed; 32].map(|byte| byte & 0x0f)).unwrap()
    }

    #[tokio::test]
    async fn connect_checks_the_network() {
        let mock = mainnet(1, |_, _| Reply::Close).await;
        let node = ZakuraClient::new(RpcClient::new(mock.config()).unwrap());
        let backend = RemoteBackend::connect(node.clone(), Network::Mainnet).await.unwrap();
        assert_eq!(backend.network(), Network::Mainnet);
        assert_eq!(backend.tip_height().await.unwrap(), Some(block::Height(1)));

        let error = RemoteBackend::connect(node, Network::new_regtest(Default::default()))
            .await
            .unwrap_err();
        assert!(
            matches!(&error, RemoteConnectError::WrongChain { expected, found }
                if expected == "test" && found == "main"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn connect_checks_the_genesis_hash() {
        let mock = MockNode::start(|_, seen| match seen.body["method"].as_str().unwrap() {
            "getblockchaininfo" => {
                Reply::Result(serde_json::to_value(GetBlockchainInfoResponse::default()).unwrap())
            }
            _ => Reply::Result(json!(block::Hash([7; 32]).to_string())),
        })
        .await;
        let node = ZakuraClient::new(RpcClient::new(mock.config()).unwrap());
        let error = RemoteBackend::connect(node, Network::Mainnet).await.unwrap_err();
        assert!(
            matches!(error, RemoteConnectError::WrongGenesis { found, .. } if found == block::Hash([7; 32]))
        );
    }

    #[tokio::test]
    async fn block_parses_the_raw_answer() {
        let mock = mainnet(1, |_, params| match params[0].as_str().unwrap() {
            "1111111111111111111111111111111111111111111111111111111111111111" => {
                Reply::Error { code: RpcCode::INVALID_PARAMETER, message: "Block not found" }
            }
            _ => {
                Reply::Result(serde_json::to_value(HexData(BLOCK_MAINNET_1_BYTES.clone())).unwrap())
            }
        })
        .await;
        let backend = backend(&mock).await;
        let expected = block_1();
        let block = backend.block(expected.hash()).await.unwrap().unwrap();
        assert_eq!(*block, expected);
        assert!(backend.block(block::Hash([0x11; 32])).await.unwrap().is_none());
        let error = backend.block(block::Hash([0; 32])).await.unwrap_err();
        assert!(matches!(error, BackendError::Unexpected(_)), "{error}");
    }

    #[tokio::test]
    async fn transaction_parses_the_verbose_answer() {
        let coinbase = block_1().transactions[0].clone();
        let id = coinbase.hash();
        let mined = Arc::new(AtomicBool::new(true));
        let in_chain = mined.clone();
        let mock = mainnet(1, move |_, _| {
            let height = in_chain.load(Ordering::SeqCst).then_some(block::Height(1));
            Reply::Result(transaction_object(&coinbase, height))
        })
        .await;
        let backend = backend(&mock).await;
        let found = backend.transaction(id).await.unwrap().unwrap();
        assert_eq!(found.transaction.hash(), id);
        assert_eq!(found.height, block::Height(1));
        // A mempool transaction is not in the best chain.
        mined.store(false, Ordering::SeqCst);
        assert!(backend.transaction(id).await.unwrap().is_none());
        // A mined answer for another id is refused.
        mined.store(true, Ordering::SeqCst);
        let error = backend.transaction(transaction::Hash([1; 32])).await.unwrap_err();
        assert!(matches!(error, BackendError::Unexpected(_)), "{error}");
    }

    /// The verbose `getrawtransaction` answer for `transaction`, mined at `height` or in the
    /// mempool.
    fn transaction_object(transaction: &Arc<Transaction>, height: Option<block::Height>) -> Value {
        serde_json::to_value(TransactionObject::from_transaction(
            transaction.clone(),
            height,
            height.map(|_| 1),
            &Network::Mainnet,
            None,
            None,
            Some(height.is_some()),
            transaction.hash(),
        ))
        .unwrap()
    }

    #[tokio::test]
    async fn subtrees_parse_each_pool_in_stored_byte_order() {
        let sapling = SaplingNode::from_bytes([3; 32]).unwrap();
        let orchard = orchard_root(5);
        let mock = mainnet(2_000_000, move |_, params| {
            let root = match params[0].as_str().unwrap() {
                "sapling" => hex::encode(sapling.to_bytes()),
                _ => hex::encode(orchard.to_repr()),
            };
            Reply::Result(json!({
                "pool": params[0],
                "start_index": params[1],
                "subtrees": [{"root": root, "end_height": 1_000_000}],
            }))
        })
        .await;
        let backend = backend(&mock).await;
        let index = NoteCommitmentSubtreeIndex(4);
        fn expected<N>(root: N) -> Subtrees<N> {
            let subtree = NoteCommitmentSubtreeData::new(block::Height(1_000_000), root);
            BTreeMap::from([(NoteCommitmentSubtreeIndex(4), subtree)])
        }
        assert_eq!(backend.sapling_subtrees(index, None).await.unwrap(), expected(sapling));
        assert_eq!(backend.orchard_subtrees(index, None).await.unwrap(), expected(orchard));
        assert_eq!(backend.ironwood_subtrees(index, None).await.unwrap(), expected(orchard));
    }

    #[tokio::test]
    async fn subtrees_stop_at_the_last_index() {
        let root = hex::encode(orchard_root(1).to_repr());
        let mock = mainnet(10_000, move |_, params| {
            let subtree = json!({"root": root, "end_height": 100});
            Reply::Result(json!({"pool": "orchard", "start_index": params[1], "subtrees": [subtree, subtree]}))
        })
        .await;
        let backend = backend(&mock).await;
        let last = NoteCommitmentSubtreeIndex(u16::MAX);
        let error = backend.orchard_subtrees(last, None).await.unwrap_err();
        assert!(matches!(&error, BackendError::Unexpected(message) if message.contains("65535")));
    }

    fn tree_state_answer(hash: block::Hash, orchard_root: Option<Vec<u8>>) -> Value {
        let sapling: sapling::tree::NoteCommitmentTree = (1..=3u8)
            .map(|i| {
                sapling::tree::NoteCommitmentUpdate::from_bytes(&[i; 32].map(|b| b & 0x0f)).unwrap()
            })
            .collect::<Vec<_>>()
            .into();
        let orchard: orchard::tree::NoteCommitmentTree =
            (1..=5).map(orchard::tree::NoteCommitmentUpdate::from).collect::<Vec<_>>().into();
        let orchard_root = orchard_root.unwrap_or(orchard.root().bytes_in_display_order().to_vec());
        let commitments =
            |root: Vec<u8>, tree| Treestate::new(Commitments::new(Some(root), Some(tree)));
        serde_json::to_value(GetTreestateResponse::new(
            hash,
            block::Height(1),
            0,
            None,
            commitments(sapling.root().bytes_in_display_order().to_vec(), sapling.to_rpc_bytes()),
            commitments(orchard_root, orchard.to_rpc_bytes()),
            Treestate::default(),
        ))
        .unwrap()
    }

    #[tokio::test]
    async fn tree_state_names_the_prune_height_of_a_pruned_node() {
        let mock = MockNode::start(|_, seen| match seen.body["method"].as_str().unwrap() {
            "getblockchaininfo" => {
                let mut info = serde_json::to_value(GetBlockchainInfoResponse::default()).unwrap();
                info["pruned"] = json!(true);
                info["pruneheight"] = json!(500);
                Reply::Result(info)
            }
            "z_gettreestate" => {
                Reply::Error { code: RpcCode::INVALID_PARAMETER, message: "not in main chain" }
            }
            method => chain_reply(method, 1_000).unwrap(),
        })
        .await;
        let error = backend(&mock).await.tree_state(block::Hash([1; 32])).await.unwrap_err();
        assert!(
            matches!(&error, BackendError::Unavailable(message) if message.contains("below height 500")),
            "{error}"
        );
    }

    #[tokio::test]
    async fn tree_state_parses_once_per_hash() {
        let (good, bad_root) = (block::Hash([1; 32]), block::Hash([2; 32]));
        let mock = mainnet(1, move |_, params| match params[0].as_str().unwrap() {
            hash if hash == good.to_string() => Reply::Result(tree_state_answer(good, None)),
            hash if hash == bad_root.to_string() => {
                Reply::Result(tree_state_answer(bad_root, Some(vec![0; 32])))
            }
            _ => Reply::Result(tree_state_answer(good, None)),
        })
        .await;
        let backend = backend(&mock).await;
        let trees = backend.tree_state(good).await.unwrap();
        assert_eq!(trees.sapling.unwrap().count(), 3);
        assert_eq!(trees.orchard.unwrap().count(), 5);
        assert!(trees.ironwood.is_none());
        assert_eq!(backend.lock_tree_states().len(), 1);
        let requests = mock.seen().len();
        assert_eq!(backend.tree_state(good).await.unwrap().orchard.unwrap().count(), 5);
        assert_eq!(mock.seen().len(), requests);

        for hash in [bad_root, block::Hash([3; 32])] {
            let error = backend.tree_state(hash).await.unwrap_err();
            assert!(matches!(error, BackendError::Unexpected(_)), "{error}");
        }
    }

    #[tokio::test]
    async fn tree_state_cache_keeps_at_most_its_cap() {
        let mock = mainnet(1, |_, _| Reply::Close).await;
        let backend = backend(&mock).await;
        let hash = |index: u32| {
            let mut bytes = [0; 32];
            bytes[..4].copy_from_slice(&index.to_le_bytes());
            block::Hash(bytes)
        };
        let cap = TREE_CACHE_ENTRIES.get() as u32;
        let mut cache = backend.lock_tree_states();
        for index in 0..cap {
            cache.put(hash(index), TreeState::default());
        }
        // A hit makes hash 0 the most recent entry, so the next insert drops hash 1.
        assert!(cache.get(&hash(0)).is_some());
        cache.put(hash(cap), TreeState::default());
        assert_eq!(cache.len(), cap as usize);
        assert!(cache.contains(&hash(0)) && cache.contains(&hash(cap)));
        assert!(!cache.contains(&hash(1)));
    }

    const ADDRESS: &str = "t1at7nVNsv6taLRrNRvnQdtfLNRDfsGc3Ak";

    fn address() -> transparent::Address {
        ADDRESS.parse().unwrap()
    }

    /// The params of the last request of the mock.
    fn last_params(mock: &MockNode) -> Value {
        mock.seen().last().unwrap().body["params"].clone()
    }

    #[tokio::test]
    async fn address_reads_parse_each_answer() {
        let txid = block_1().transactions[0].hash();
        let mock = mainnet(1, move |method, _| match method {
            "getaddresstxids" => Reply::Result(json!([txid.to_string()])),
            "getaddressbalance" => Reply::Result(json!({"balance": 5, "received": 7})),
            _ => Reply::Result(
                serde_json::to_value(vec![Utxo::new(
                    ADDRESS.parse().unwrap(),
                    txid,
                    transparent::OutputIndex::from_index(2),
                    transparent::Script::new(&[0x51]),
                    9,
                    block::Height(1),
                )])
                .unwrap(),
            ),
        })
        .await;
        let backend = backend(&mock).await;
        let heights = block::Height(1)..=block::Height(5);
        assert_eq!(backend.address_transaction_ids(address(), heights).await.unwrap(), [txid]);
        assert_eq!(last_params(&mock), json!([{"addresses": [ADDRESS], "start": 1, "end": 5}]));
        let requests = mock.seen().len();
        let genesis = block::Height(0)..=block::Height(0);
        assert!(backend.address_transaction_ids(address(), genesis).await.unwrap().is_empty());
        assert_eq!(mock.seen().len(), requests);
        // The tip of the mock is 1, so a range from 2 has no transactions.
        let above = block::Height(2)..=block::Height(9);
        assert!(backend.address_transaction_ids(address(), above).await.unwrap().is_empty());
        assert_eq!(mock.seen().last().unwrap().body["method"], "getblockcount");

        let addresses = HashSet::from([address()]);
        assert_eq!(
            backend.address_balance(addresses.clone()).await.unwrap(),
            Amount::<NonNegative>::try_from(5_u64).unwrap()
        );
        assert_eq!(last_params(&mock), json!([{"addresses": [ADDRESS]}]));
        assert_eq!(
            backend.address_utxos(addresses).await.unwrap(),
            [AddressUtxo {
                address: address(),
                outpoint: transparent::OutPoint { hash: txid, index: 2 },
                output: transparent::Output {
                    value: Amount::<NonNegative>::try_from(9_u64).unwrap(),
                    lock_script: transparent::Script::new(&[0x51]),
                },
                height: block::Height(1),
            }]
        );
        assert_eq!(last_params(&mock), json!([{"addresses": [ADDRESS], "chainInfo": false}]));
    }

    #[tokio::test]
    async fn mempool_caches_and_skips_what_is_not_pending() {
        let block = block_1();
        let mined: Block = BLOCK_MAINNET_2_BYTES.zcash_deserialize_into().unwrap();
        let (pending, mined) = (block.transactions[0].clone(), mined.transactions[0].clone());
        let (pending_id, mined_id) = (pending.hash(), mined.hash());
        let (gone, failing) = (transaction::Hash([1; 32]), transaction::Hash([2; 32]));
        let mock = mainnet(1, move |method, params| {
            let id = params[0].as_str().unwrap_or_default();
            match method {
                "getrawmempool" => Reply::Result(json!(
                    [pending_id, mined_id, gone, failing].map(|id| id.to_string())
                )),
                _ if id == pending_id.to_string() => {
                    Reply::Result(transaction_object(&pending, None))
                }
                _ if id == mined_id.to_string() => {
                    Reply::Result(transaction_object(&mined, Some(block::Height(2))))
                }
                _ if id == failing.to_string() => Reply::Close,
                _ => Reply::Error { code: RpcCode::INVALID_ADDRESS_OR_KEY, message: "not found" },
            }
        })
        .await;
        let backend = backend(&mock).await;
        let ids = |mempool: Vec<UnminedTx>| {
            mempool.iter().map(|tx| tx.id().mined_id()).collect::<Vec<_>>()
        };
        assert_eq!(ids(backend.mempool_transactions().await.unwrap()), [pending_id]);

        // The second read asks again only for the transactions that are not in the cache.
        let requests = mock.seen().len();
        assert_eq!(ids(backend.mempool_transactions().await.unwrap()), [pending_id]);
        let asked: HashSet<_> = mock.seen()[requests..]
            .iter()
            .filter(|seen| seen.body["method"] == "getrawtransaction")
            .map(|seen| seen.body["params"][0].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(asked, HashSet::from([mined_id, gone, failing].map(|id| id.to_string())));
    }

    #[tokio::test]
    async fn mempool_cache_keeps_only_the_listed_ids() {
        let pending = block_1().transactions[0].clone();
        let id = pending.hash();
        let listed = Arc::new(AtomicBool::new(true));
        let in_mempool = listed.clone();
        let mock = mainnet(1, move |method, _| match method {
            "getrawmempool" if in_mempool.load(Ordering::SeqCst) => {
                Reply::Result(json!([id.to_string()]))
            }
            "getrawmempool" => Reply::Result(json!([])),
            _ => Reply::Result(transaction_object(&pending, None)),
        })
        .await;
        let backend = backend(&mock).await;
        assert_eq!(backend.mempool_transactions().await.unwrap().len(), 1);
        assert!(backend.lock_mempool().contains_key(&id));

        // The transaction left the mempool, so the cache drops it.
        listed.store(false, Ordering::SeqCst);
        assert!(backend.mempool_transactions().await.unwrap().is_empty());
        assert!(backend.lock_mempool().is_empty());
    }

    #[tokio::test]
    async fn submit_maps_a_node_error_to_rejected() {
        let coinbase = block_1().transactions[0].clone();
        let id = coinbase.hash();
        let mock = mainnet(1, move |_, _| Reply::Result(json!(id.to_string()))).await;
        let accepting = backend(&mock).await;
        assert_eq!(accepting.submit_transaction((*coinbase).clone()).await.unwrap(), id);
        assert_eq!(
            last_params(&mock),
            json!([hex::encode(coinbase.zcash_serialize_to_vec().unwrap())])
        );

        let mock =
            mainnet(1, |_, _| Reply::Error { code: RpcCode(-26), message: "bad-txns" }).await;
        let error = backend(&mock).await.submit_transaction((*coinbase).clone()).await.unwrap_err();
        assert!(matches!(&error, BackendError::Rejected(message) if message.contains("bad-txns")));
        // A node in warm-up says nothing about the transaction.
        let mock =
            mainnet(1, |_, _| Reply::Error { code: RpcCode::IN_WARMUP, message: "warm-up" }).await;
        let error = backend(&mock).await.submit_transaction((*coinbase).clone()).await.unwrap_err();
        assert!(matches!(error, BackendError::Unavailable(_)), "{error}");
    }
}
