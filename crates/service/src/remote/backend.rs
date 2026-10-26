//! A [`StateBackend`] over the JSON-RPC of a remote zakura node.
//!
//! Each method calls the node through [`ZakuraClient`] and parses the answer into the
//! `zakura_chain` type of the trait. A parse failure is [`BackendError::Unexpected`].
//!
//! Some answers do not change once the node gives them. The backend keeps them in a bounded
//! cache, and a cache hit makes no call to the node.

use std::{
    collections::HashSet,
    hash::Hash,
    mem::size_of,
    ops::RangeInclusive,
    sync::{Arc, Mutex, PoisonError},
};

use lru::LruCache;
use sapling_crypto::Node as SaplingNode;
use zakura_chain::{
    amount::{Amount, NonNegative},
    block::{self, Block},
    ironwood, orchard,
    parameters::Network,
    serialization::{ZcashDeserialize, ZcashDeserializeInto as _},
    subtree::{NoteCommitmentSubtreeData, NoteCommitmentSubtreeIndex},
    transaction::{self, Transaction, UnminedTx},
    transparent,
};
use zakura_rpc::client::{GetRawTransactionResponse, GetSubtreesByIndexResponse};
use zakura_state::{HashOrHeight, MAX_BLOCK_REORG_HEIGHT};
use ztreamer_remote::{RpcCode, RpcError, ZakuraClient};

use crate::backend::{
    BackendError, MinedTransaction, StateBackend, Subtrees, TipListener, TreeState,
};

/// The bytes that the subtree cache of one pool can hold: approximately 16,000 subtrees.
const SUBTREE_CACHE_BYTES: usize = 2 << 20;

/// The PR that adds the methods that this backend does not serve yet.
const NEXT_PR: &str = "feat(service): RemoteBackend addresses, mempool and submit";

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
    sapling_subtrees: SubtreeCache<SaplingNode>,
    orchard_subtrees: SubtreeCache<orchard::tree::Node>,
    ironwood_subtrees: SubtreeCache<ironwood::tree::Node>,
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
            sapling_subtrees: LruMap::new("sapling_subtrees", SUBTREE_CACHE_BYTES),
            orchard_subtrees: LruMap::new("orchard_subtrees", SUBTREE_CACHE_BYTES),
            ironwood_subtrees: LruMap::new("ironwood_subtrees", SUBTREE_CACHE_BYTES),
        })
    }

    /// Completed subtrees of `P` from `start`, at most `limit` of them.
    ///
    /// The subtrees in the cache from `start` on make the start of the answer. The node gives
    /// only the rest. The cache keeps a subtree when it is at least [`MAX_BLOCK_REORG_HEIGHT`]
    /// blocks below the tip, because a reorg cannot change it then.
    async fn subtrees<P: Pool>(
        &self,
        start: NoteCommitmentSubtreeIndex,
        limit: Option<NoteCommitmentSubtreeIndex>,
    ) -> Result<Subtrees<P::Node>, BackendError> {
        let cache = P::cache(self);
        // Index arithmetic is in `u32`, because a `u16` range cannot step past index 65535.
        let end = limit.map_or(1 << 16, |limit| u32::from(start.0) + u32::from(limit.0));
        let mut subtrees: Subtrees<P::Node> = (u32::from(start.0)..end)
            .map_while(|index| u16::try_from(index).ok())
            .map_while(|index| cache.get(&index).map(|subtree| (index, subtree)))
            .map(|(index, subtree)| (NoteCommitmentSubtreeIndex(index), subtree))
            .collect();
        let next =
            subtrees.last_key_value().map_or(u32::from(start.0), |(last, _)| u32::from(last.0) + 1);
        let Some(next) = u16::try_from(next).ok().filter(|&next| u32::from(next) < end) else {
            return Ok(subtrees);
        };
        let remaining = limit.map(|_| NoteCommitmentSubtreeIndex((end - u32::from(next)) as u16));
        let next = NoteCommitmentSubtreeIndex(next);
        let (response, tip) = tokio::join!(
            self.node.z_get_subtrees_by_index(P::NAME, next, remaining),
            self.tip_height(),
        );
        let fetched = P::parse_subtrees(response?, next)?;
        // The tip only decides what goes in the cache. Without it, the answer is still good, and
        // nothing goes in the cache.
        let tip = tip
            .inspect_err(
                |error| tracing::debug!(%error, "remote zakura: no tip for the subtree cache"),
            )
            .ok()
            .flatten();
        let final_below = tip.map_or(0, |tip| tip.0.saturating_sub(MAX_BLOCK_REORG_HEIGHT));
        fetched
            .iter()
            .filter(|(_, subtree)| subtree.end_height.0 <= final_below)
            .for_each(|(index, subtree)| cache.insert(index.0, subtree.clone()));
        subtrees.extend(fetched);
        Ok(subtrees)
    }

    fn not_yet<T>(what: &str) -> Result<T, BackendError> {
        Err(BackendError::Unavailable(format!(
            "RemoteBackend does not serve {what} yet; the PR \"{NEXT_PR}\" adds it"
        )))
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

/// Turns the node's not-found answer into `None`.
trait OrNotFound<T> {
    fn or_not_found(self, code: RpcCode) -> Result<Option<T>, BackendError>;
}

impl<T> OrNotFound<T> for Result<T, RpcError> {
    fn or_not_found(self, code: RpcCode) -> Result<Option<T>, BackendError> {
        match self {
            Ok(value) => Ok(Some(value)),
            Err(error) if error.rpc_code() == Some(code) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
}

/// A shielded pool: its name in `z_getsubtreesbyindex`, its root type and its cache.
trait Pool {
    const NAME: &'static str;
    type Node: Clone + Send + Sync + 'static;

    fn root(bytes: [u8; 32]) -> Option<Self::Node>;
    fn cache(backend: &RemoteBackend) -> &SubtreeCache<Self::Node>;

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
                let root = hex::decode(&subtree.root)
                    .ok()
                    .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
                    .and_then(Self::root)
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
    const NAME: &'static str = "sapling";
    type Node = SaplingNode;

    fn root(bytes: [u8; 32]) -> Option<Self::Node> {
        SaplingNode::from_bytes(bytes).into()
    }

    fn cache(backend: &RemoteBackend) -> &SubtreeCache<Self::Node> {
        &backend.sapling_subtrees
    }
}

impl Pool for OrchardPool {
    const NAME: &'static str = "orchard";
    type Node = orchard::tree::Node;

    fn root(bytes: [u8; 32]) -> Option<Self::Node> {
        Self::Node::try_from(bytes).ok()
    }

    fn cache(backend: &RemoteBackend) -> &SubtreeCache<Self::Node> {
        &backend.orchard_subtrees
    }
}

impl Pool for IronwoodPool {
    const NAME: &'static str = "ironwood";
    type Node = ironwood::tree::Node;

    fn root(bytes: [u8; 32]) -> Option<Self::Node> {
        Self::Node::try_from(bytes).ok()
    }

    fn cache(backend: &RemoteBackend) -> &SubtreeCache<Self::Node> {
        &backend.ironwood_subtrees
    }
}

/// A map in front of node answers that holds at most a budget of bytes. When an insert goes
/// above the budget, the map drops the entries that were used least recently.
///
/// The bytes of an entry are an estimate: the size of the key and the value, a fixed overhead
/// for the map, and [`HeapBytes::heap_bytes`] of the value. The map reports its bytes, its
/// entries, and its hits, misses and evictions as metrics with the label `cache`.
struct LruMap<K, V> {
    budget: usize,
    inner: Mutex<LruMapInner<K, V>>,
    metrics: CacheMetrics,
}

struct LruMapInner<K, V> {
    entries: LruCache<K, (V, usize)>,
    bytes: usize,
}

/// Completed subtrees of one pool by index.
type SubtreeCache<Node> = LruMap<u16, NoteCommitmentSubtreeData<Node>>;

/// The bytes that an entry of [`LruMap`] uses for the hash table and the list links.
const ENTRY_OVERHEAD: usize = 64;

/// The heap bytes of a cached value, as an estimate.
trait HeapBytes {
    fn heap_bytes(&self) -> usize;
}

impl<Node> HeapBytes for NoteCommitmentSubtreeData<Node> {
    fn heap_bytes(&self) -> usize {
        0
    }
}

impl<K: Hash + Eq, V: Clone + HeapBytes> LruMap<K, V> {
    fn new(name: &'static str, budget: usize) -> Self {
        Self {
            budget,
            inner: Mutex::new(LruMapInner { entries: LruCache::unbounded(), bytes: 0 }),
            metrics: CacheMetrics::new(name),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, LruMapInner<K, V>> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn get(&self, key: &K) -> Option<V> {
        let value = self.lock().entries.get(key).map(|(value, _)| value.clone());
        match value {
            Some(_) => self.metrics.hits.increment(1),
            None => self.metrics.misses.increment(1),
        }
        value
    }

    fn insert(&self, key: K, value: V) {
        let bytes = size_of::<(K, V, usize)>() + ENTRY_OVERHEAD + value.heap_bytes();
        let mut inner = self.lock();
        if let Some((_, (_, old))) = inner.entries.push(key, (value, bytes)) {
            inner.bytes -= old;
        }
        inner.bytes += bytes;
        while inner.bytes > self.budget {
            let Some((_, (_, dropped))) = inner.entries.pop_lru() else {
                break;
            };
            inner.bytes -= dropped;
            self.metrics.evictions.increment(1);
        }
        self.metrics.report(&inner);
    }
}

/// The metrics of one [`LruMap`].
struct CacheMetrics {
    bytes: metrics::Gauge,
    entries: metrics::Gauge,
    hits: metrics::Counter,
    misses: metrics::Counter,
    evictions: metrics::Counter,
}

impl CacheMetrics {
    fn new(cache: &'static str) -> Self {
        Self {
            bytes: metrics::gauge!("ztreamer.remote_cache.bytes", "cache" => cache),
            entries: metrics::gauge!("ztreamer.remote_cache.entries", "cache" => cache),
            hits: metrics::counter!("ztreamer.remote_cache.hits_total", "cache" => cache),
            misses: metrics::counter!("ztreamer.remote_cache.misses_total", "cache" => cache),
            evictions: metrics::counter!("ztreamer.remote_cache.evictions_total", "cache" => cache),
        }
    }

    fn report<K: Hash + Eq, V>(&self, inner: &LruMapInner<K, V>) {
        self.bytes.set(inner.bytes as f64);
        self.entries.set(inner.entries.len() as f64);
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
        let request = self.node.get_block_raw(HashOrHeight::Hash(hash)).await;
        let Some(raw) = request.or_not_found(RpcCode::INVALID_PARAMETER)? else {
            return Ok(None);
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
        let request = self.node.get_raw_transaction(hash, true).await;
        let Some(response) = request.or_not_found(RpcCode::INVALID_ADDRESS_OR_KEY)? else {
            return Ok(None);
        };
        let GetRawTransactionResponse::Object(object) = response else {
            return Err(BackendError::Unexpected(
                "remote zakura sent a raw transaction for a verbose request".into(),
            ));
        };
        // No height: the transaction is in the mempool. A height of -1: it is in a side chain.
        let Some(height) = object.height().and_then(|height| u32::try_from(height).ok()) else {
            return Ok(None);
        };
        let transaction = Transaction::from_node_bytes(object.hex().as_ref().to_vec()).await?;
        if transaction.hash() != hash {
            return Err(BackendError::Unexpected(format!(
                "remote zakura sent transaction {} for {hash}",
                transaction.hash()
            )));
        }
        Ok(Some(MinedTransaction {
            transaction: Arc::new(transaction),
            height: block::Height(height),
        }))
    }

    async fn tree_state(&self, _hash: block::Hash) -> Result<TreeState, BackendError> {
        Err(BackendError::Unavailable(
            "RemoteBackend does not serve tree states yet; it needs the z_gettreestate parser of \
             cypherpunktech/ztreamer#114"
                .into(),
        ))
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
        _address: transparent::Address,
        _heights: RangeInclusive<block::Height>,
    ) -> Result<Vec<transaction::Hash>, BackendError> {
        Self::not_yet("address transactions")
    }

    async fn address_balance(
        &self,
        _addresses: HashSet<transparent::Address>,
    ) -> Result<Amount<NonNegative>, BackendError> {
        Self::not_yet("address balances")
    }

    async fn address_utxos(
        &self,
        _addresses: HashSet<transparent::Address>,
    ) -> Result<Vec<crate::backend::AddressUtxo>, BackendError> {
        Self::not_yet("address UTXOs")
    }

    async fn mempool_transactions(&self) -> Result<Vec<UnminedTx>, BackendError> {
        Self::not_yet("the mempool")
    }

    async fn submit_transaction(
        &self,
        _transaction: Transaction,
    ) -> Result<transaction::Hash, BackendError> {
        Self::not_yet("transaction submission")
    }

    fn tip_listener(&self) -> Result<Box<dyn TipListener>, BackendError> {
        Self::not_yet("tip changes")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use super::*;
    use serde_json::{Value, json};
    use zakura_rpc::client::{GetBlockchainInfoResponse, HexData, TransactionObject};
    use zakura_test::vectors::BLOCK_MAINNET_1_BYTES;
    use ztreamer_remote::{
        RpcClient,
        test_utils::{MockNode, Reply},
    };

    /// A mock mainnet node with tip `tip`. It answers other methods from `answer`.
    async fn mainnet(
        tip: u32,
        answer: impl Fn(&str, &Value) -> Reply + Send + Sync + 'static,
    ) -> MockNode {
        MockNode::start(move |_, seen| {
            let (method, params) = (seen.body["method"].as_str().unwrap(), &seen.body["params"]);
            match method {
                "getblockchaininfo" => {
                    let mut info =
                        serde_json::to_value(GetBlockchainInfoResponse::default()).unwrap();
                    info["blocks"] = json!(tip);
                    Reply::Result(info)
                }
                "getblockcount" => Reply::Result(json!(tip)),
                "getblockhash" => Reply::Result(json!(Network::Mainnet.genesis_hash().to_string())),
                _ => answer(method, params),
            }
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
        let mock = mainnet(1, move |_, params| {
            let mined = params[0].as_str().unwrap() == id.to_string();
            let height = mined.then_some(block::Height(1));
            Reply::Result(
                serde_json::to_value(TransactionObject::from_transaction(
                    coinbase.clone(),
                    height,
                    height.map(|_| 1),
                    &Network::Mainnet,
                    None,
                    None,
                    Some(mined),
                    id,
                ))
                .unwrap(),
            )
        })
        .await;
        let backend = backend(&mock).await;
        let mined = backend.transaction(id).await.unwrap().unwrap();
        assert_eq!(mined.transaction.hash(), id);
        assert_eq!(mined.height, block::Height(1));
        // The node answers with an unmined transaction, so it is not in the best chain.
        assert!(backend.transaction(transaction::Hash([1; 32])).await.unwrap().is_none());
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

    #[tokio::test]
    async fn subtrees_without_a_tip_are_served_and_not_cached() {
        let root = hex::encode(orchard_root(1).to_repr());
        let mock = MockNode::start(move |_, seen| match seen.body["method"].as_str().unwrap() {
            "getblockcount" => Reply::Close,
            "z_getsubtreesbyindex" => Reply::Result(json!({
                "pool": "orchard",
                "start_index": 0,
                "subtrees": [{"root": root, "end_height": 100}],
            })),
            "getblockchaininfo" => {
                Reply::Result(serde_json::to_value(GetBlockchainInfoResponse::default()).unwrap())
            }
            _ => Reply::Result(json!(Network::Mainnet.genesis_hash().to_string())),
        })
        .await;
        let backend = backend(&mock).await;
        let one = Some(NoteCommitmentSubtreeIndex(1));
        assert_eq!(
            backend.orchard_subtrees(NoteCommitmentSubtreeIndex(0), one).await.unwrap().len(),
            1
        );
        let requests = mock.seen().len();
        backend.orchard_subtrees(NoteCommitmentSubtreeIndex(0), one).await.unwrap();
        assert!(mock.seen().len() > requests);
    }

    #[test]
    fn lru_map_keeps_its_byte_budget_and_reports_it() {
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let map: SubtreeCache<orchard::tree::Node> =
            metrics::with_local_recorder(&recorder, || LruMap::new("test", 1_000));
        let entry = size_of::<(u16, NoteCommitmentSubtreeData<orchard::tree::Node>, usize)>()
            + ENTRY_OVERHEAD;
        let subtree = NoteCommitmentSubtreeData::new(block::Height(1), orchard_root(1));
        (0..100).for_each(|index| map.insert(index, subtree));
        let kept = 1_000 / entry;
        assert_eq!(map.lock().entries.len(), kept);
        assert!(map.lock().bytes <= 1_000);
        // The oldest entries are gone, the newest stay.
        assert!(map.get(&0).is_none());
        assert!(map.get(&99).is_some());

        let values: HashMap<_, _> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .map(|(key, _, _, value)| (key.key().name().to_owned(), value))
            .collect();
        use metrics_util::debugging::DebugValue;
        assert_eq!(
            values["ztreamer.remote_cache.entries"],
            DebugValue::Gauge((kept as f64).into())
        );
        assert_eq!(
            values["ztreamer.remote_cache.bytes"],
            DebugValue::Gauge(((kept * entry) as f64).into())
        );
        assert_eq!(
            values["ztreamer.remote_cache.evictions_total"],
            DebugValue::Counter(100 - kept as u64)
        );
        assert_eq!(values["ztreamer.remote_cache.hits_total"], DebugValue::Counter(1));
        assert_eq!(values["ztreamer.remote_cache.misses_total"], DebugValue::Counter(1));
    }

    #[tokio::test]
    async fn cached_subtrees_make_no_request() {
        // Subtree 0 is final. Subtree 1 is 10 blocks below the tip.
        let mock = mainnet(10_000, |_, params| {
            let subtrees: Vec<_> = [(0, 100), (1, 9_990)][params[1].as_u64().unwrap() as usize..]
                .iter()
                .map(|&(seed, end)| json!({"root": hex::encode(orchard_root(seed).to_repr()), "end_height": end}))
                .collect();
            Reply::Result(json!({"pool": "orchard", "start_index": params[1], "subtrees": subtrees}))
        })
        .await;
        let backend = backend(&mock).await;
        let start = NoteCommitmentSubtreeIndex(0);
        let all = backend.orchard_subtrees(start, None).await.unwrap();
        assert_eq!(all.len(), 2);
        let requests = mock.seen().len();

        let one = Some(NoteCommitmentSubtreeIndex(1));
        assert_eq!(backend.orchard_subtrees(start, one).await.unwrap().len(), 1);
        assert_eq!(mock.seen().len(), requests);

        // Subtree 1 is not final, so the node gives it again, from index 1.
        assert_eq!(backend.orchard_subtrees(start, None).await.unwrap(), all);
        let sent = mock.seen();
        let subtree_requests: Vec<_> = sent[requests..]
            .iter()
            .filter(|seen| seen.body["method"] == "z_getsubtreesbyindex")
            .collect();
        assert_eq!(subtree_requests.len(), 1);
        assert_eq!(subtree_requests[0].body["params"], json!(["orchard", 1]));
    }

    #[tokio::test]
    async fn methods_of_the_next_pr_are_unavailable() {
        let mock = mainnet(1, |_, _| Reply::Close).await;
        let backend = backend(&mock).await;
        let requests = mock.seen().len();
        let error = backend.mempool_transactions().await.unwrap_err();
        assert!(matches!(&error, BackendError::Unavailable(message) if message.contains(NEXT_PR)));
        assert!(backend.tree_state(block::Hash([0; 32])).await.is_err());
        assert_eq!(mock.seen().len(), requests);
    }
}
