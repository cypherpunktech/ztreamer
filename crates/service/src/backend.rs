//! The chain-state seam: everything [`CompactService`](crate::CompactService) needs that is not
//! in the compact index.
//!
//! The compact index answers block and range requests. Everything else, from transparent address
//! lookups to tree states to the mempool, comes from a node. Today that node is the embedded
//! Zakura, reached in-process through its `ReadStateService` and the mempool handle; that is
//! [`InProcessBackend`]. A backend over a remote node's JSON-RPC implements the same trait.
//!
//! The trait speaks in `zakura_chain` types, not in `ReadResponse` variants and not in the wire
//! encodings of the light wallet protocol. The service does the wire conversion. A remote
//! backend parses the node's RPC answers into the same types, which also validates them.

use std::{
    collections::{BTreeMap, HashSet},
    ops::RangeInclusive,
    sync::Arc,
};

use sapling_crypto::Node as SaplingNode;
use tonic::Status;
use tower::ServiceExt as _;
use zakura_chain::{
    amount::{self, Amount, NonNegative},
    block::{self, Block},
    ironwood, orchard,
    parameters::Network,
    sapling,
    subtree::{NoteCommitmentSubtreeData, NoteCommitmentSubtreeIndex},
    transaction::{self, Transaction, UnminedTx},
    transparent,
};
use zakura_state::{ReadRequest, ReadResponse, ReadStateService};
use ztreamer_node::NodeClient;

/// Why a backend request failed. Each variant maps to one gRPC status code.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum BackendError {
    /// The backend cannot answer now: the node is unreachable, a read failed, or this backend
    /// has no node handle for the request. Maps to `UNAVAILABLE`.
    #[error("{0}")]
    Unavailable(String),
    /// The node refused the request's content, for example a transaction that failed
    /// verification. Maps to `INVALID_ARGUMENT`.
    #[error("{0}")]
    Rejected(String),
    /// The backend answered with something it should not have. Maps to `INTERNAL`.
    #[error("{0}")]
    Unexpected(String),
}

/// A backend answer with an amount out of range is an answer that it should not have sent.
impl From<amount::Error> for BackendError {
    fn from(error: amount::Error) -> Self {
        Self::Unexpected(format!("backend sent an amount out of range: {error}"))
    }
}

impl From<BackendError> for Status {
    fn from(error: BackendError) -> Self {
        match error {
            BackendError::Unavailable(message) => Self::unavailable(message),
            BackendError::Rejected(message) => Self::invalid_argument(message),
            BackendError::Unexpected(message) => Self::internal(message),
        }
    }
}

/// A transaction in the best chain and the height it was mined at.
#[derive(Clone, Debug)]
pub struct MinedTransaction {
    /// The transaction.
    pub transaction: Arc<Transaction>,
    /// The height of the block that holds it.
    pub height: block::Height,
}

/// The three note commitment trees at one block. A tree is `None` when the pool was not active
/// at that block.
#[derive(Clone, Debug, Default)]
pub struct TreeState {
    /// Sapling tree.
    pub sapling: Option<Arc<sapling::tree::NoteCommitmentTree>>,
    /// Orchard tree.
    pub orchard: Option<Arc<orchard::tree::NoteCommitmentTree>>,
    /// Ironwood tree.
    pub ironwood: Option<Arc<ironwood::tree::NoteCommitmentTree>>,
}

/// Completed note commitment subtrees of one pool, by subtree index.
pub type Subtrees<Node> = BTreeMap<NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<Node>>;

/// One unspent transparent output.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AddressUtxo {
    /// The address the output pays to.
    pub address: transparent::Address,
    /// The transaction and index that identify the output.
    pub outpoint: transparent::OutPoint,
    /// The output's value and lock script.
    pub output: transparent::Output,
    /// The height the transaction was mined at.
    pub height: block::Height,
}

/// Chain state and node access for the service, behind one interface.
///
/// Every method is a point-in-time read; none of them hold a lock across the call. Methods that
/// need the mempool fail with [`BackendError::Unavailable`] on a backend that has no mempool
/// access.
#[tonic::async_trait]
pub trait StateBackend: Send + Sync + 'static {
    /// The network the node runs on.
    fn network(&self) -> Network;

    /// The node's best chain tip height, or `None` on an empty chain.
    async fn tip_height(&self) -> Result<Option<block::Height>, BackendError>;

    /// A full block from the best chain by hash. `None` when the block is not in the best chain.
    async fn block(&self, hash: block::Hash) -> Result<Option<Arc<Block>>, BackendError>;

    /// A mined transaction by id. `None` when it is not in the best chain.
    async fn transaction(
        &self,
        hash: transaction::Hash,
    ) -> Result<Option<MinedTransaction>, BackendError>;

    /// The commitment trees at the block with this hash.
    async fn tree_state(&self, hash: block::Hash) -> Result<TreeState, BackendError>;

    /// Completed Sapling subtrees from `start` on, at most `limit` of them.
    async fn sapling_subtrees(
        &self,
        start: NoteCommitmentSubtreeIndex,
        limit: Option<NoteCommitmentSubtreeIndex>,
    ) -> Result<Subtrees<SaplingNode>, BackendError>;

    /// Completed Orchard subtrees from `start` on, at most `limit` of them.
    async fn orchard_subtrees(
        &self,
        start: NoteCommitmentSubtreeIndex,
        limit: Option<NoteCommitmentSubtreeIndex>,
    ) -> Result<Subtrees<orchard::tree::Node>, BackendError>;

    /// Completed Ironwood subtrees from `start` on, at most `limit` of them.
    async fn ironwood_subtrees(
        &self,
        start: NoteCommitmentSubtreeIndex,
        limit: Option<NoteCommitmentSubtreeIndex>,
    ) -> Result<Subtrees<ironwood::tree::Node>, BackendError>;

    /// Ids of the transactions that touch `address` within the height range, in chain order.
    async fn address_transaction_ids(
        &self,
        address: transparent::Address,
        heights: RangeInclusive<block::Height>,
    ) -> Result<Vec<transaction::Hash>, BackendError>;

    /// The summed balance of the addresses.
    async fn address_balance(
        &self,
        addresses: HashSet<transparent::Address>,
    ) -> Result<Amount<NonNegative>, BackendError>;

    /// Every unspent output of the addresses, in chain order.
    async fn address_utxos(
        &self,
        addresses: HashSet<transparent::Address>,
    ) -> Result<Vec<AddressUtxo>, BackendError>;

    /// Every transaction in the node's mempool.
    async fn mempool_transactions(&self) -> Result<Vec<UnminedTx>, BackendError>;

    /// Verifies and queues a transaction. Returns its id.
    async fn submit_transaction(
        &self,
        transaction: Transaction,
    ) -> Result<transaction::Hash, BackendError>;
}

/// The embedded Zakura node, reached in-process.
///
/// Built from a `ReadStateService` alone it serves chain reads and fails mempool requests;
/// built from a [`NodeClient`] it serves everything.
#[derive(Clone)]
pub struct InProcessBackend {
    read: ReadStateService,
    node: Option<NodeClient>,
}

impl std::fmt::Debug for InProcessBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InProcessBackend")
            .field("node", &self.node.is_some())
            .finish_non_exhaustive()
    }
}

impl InProcessBackend {
    /// Chain reads only. Mempool requests fail with [`BackendError::Unavailable`].
    pub const fn new(read: ReadStateService) -> Self {
        Self { read, node: None }
    }

    /// Chain reads and mempool access through the node's handles.
    pub fn with_node(node: NodeClient) -> Self {
        Self { read: node.read_state(), node: Some(node) }
    }

    fn node(&self) -> Result<&NodeClient, BackendError> {
        self.node.as_ref().ok_or_else(|| {
            BackendError::Unavailable("embedded Zakura node handle is unavailable".into())
        })
    }

    async fn read(&self, request: ReadRequest) -> Result<ReadResponse, BackendError> {
        self.read
            .clone()
            .oneshot(request)
            .await
            .map_err(|error| BackendError::Unavailable(format!("Zakura read failed: {error}")))
    }
}

fn unexpected(what: &str) -> BackendError {
    BackendError::Unexpected(format!("unexpected {what} response"))
}

/// Zakura's mempool futures are not `Send`, so they run on a blocking thread with their own
/// `block_on`.
async fn block_on_node<T, E, F>(call: F) -> Result<T, BackendError>
where
    F: FnOnce() -> Result<T, E> + Send + 'static,
    T: Send + 'static,
    E: std::fmt::Display + Send + 'static,
{
    tokio::task::spawn_blocking(call)
        .await
        .map_err(|error| BackendError::Unavailable(format!("Zakura task failed: {error}")))?
        .map_err(|error| BackendError::Unavailable(error.to_string()))
}

#[tonic::async_trait]
impl StateBackend for InProcessBackend {
    fn network(&self) -> Network {
        self.read.db().network()
    }

    async fn tip_height(&self) -> Result<Option<block::Height>, BackendError> {
        match self.read(ReadRequest::Tip).await? {
            ReadResponse::Tip(tip) => Ok(tip.map(|(height, _)| height)),
            _ => Err(unexpected("tip")),
        }
    }

    async fn block(&self, hash: block::Hash) -> Result<Option<Arc<Block>>, BackendError> {
        match self.read(ReadRequest::Block(hash.into())).await? {
            ReadResponse::Block(block) => Ok(block),
            _ => Err(unexpected("block")),
        }
    }

    async fn transaction(
        &self,
        hash: transaction::Hash,
    ) -> Result<Option<MinedTransaction>, BackendError> {
        match self.read(ReadRequest::Transaction(hash)).await? {
            ReadResponse::Transaction(Some(mined)) => {
                Ok(Some(MinedTransaction { transaction: mined.tx, height: mined.height }))
            }
            ReadResponse::Transaction(None) => Ok(None),
            _ => Err(unexpected("transaction")),
        }
    }

    async fn tree_state(&self, hash: block::Hash) -> Result<TreeState, BackendError> {
        let (sapling, orchard, ironwood) = tokio::try_join!(
            self.read(ReadRequest::SaplingTree(hash.into())),
            self.read(ReadRequest::OrchardTree(hash.into())),
            self.read(ReadRequest::IronwoodTree(hash.into())),
        )?;
        let ReadResponse::SaplingTree(sapling) = sapling else {
            return Err(unexpected("Sapling tree"));
        };
        let ReadResponse::OrchardTree(orchard) = orchard else {
            return Err(unexpected("Orchard tree"));
        };
        let ReadResponse::IronwoodTree(ironwood) = ironwood else {
            return Err(unexpected("Ironwood tree"));
        };
        Ok(TreeState { sapling, orchard, ironwood })
    }

    async fn sapling_subtrees(
        &self,
        start_index: NoteCommitmentSubtreeIndex,
        limit: Option<NoteCommitmentSubtreeIndex>,
    ) -> Result<Subtrees<SaplingNode>, BackendError> {
        match self.read(ReadRequest::SaplingSubtrees { start_index, limit }).await? {
            ReadResponse::SaplingSubtrees(subtrees) => Ok(subtrees),
            _ => Err(unexpected("Sapling subtree")),
        }
    }

    async fn orchard_subtrees(
        &self,
        start_index: NoteCommitmentSubtreeIndex,
        limit: Option<NoteCommitmentSubtreeIndex>,
    ) -> Result<Subtrees<orchard::tree::Node>, BackendError> {
        match self.read(ReadRequest::OrchardSubtrees { start_index, limit }).await? {
            ReadResponse::OrchardSubtrees(subtrees) => Ok(subtrees),
            _ => Err(unexpected("Orchard subtree")),
        }
    }

    async fn ironwood_subtrees(
        &self,
        start_index: NoteCommitmentSubtreeIndex,
        limit: Option<NoteCommitmentSubtreeIndex>,
    ) -> Result<Subtrees<ironwood::tree::Node>, BackendError> {
        match self.read(ReadRequest::IronwoodSubtrees { start_index, limit }).await? {
            ReadResponse::IronwoodSubtrees(subtrees) => Ok(subtrees),
            _ => Err(unexpected("Ironwood subtree")),
        }
    }

    async fn address_transaction_ids(
        &self,
        address: transparent::Address,
        heights: RangeInclusive<block::Height>,
    ) -> Result<Vec<transaction::Hash>, BackendError> {
        let request = ReadRequest::TransactionIdsByAddresses {
            addresses: HashSet::from([address]),
            height_range: heights,
        };
        match self.read(request).await? {
            ReadResponse::AddressesTransactionIds(ids) => Ok(ids.into_values().collect()),
            _ => Err(unexpected("address transaction")),
        }
    }

    async fn address_balance(
        &self,
        addresses: HashSet<transparent::Address>,
    ) -> Result<Amount<NonNegative>, BackendError> {
        match self.read(ReadRequest::AddressBalance(addresses)).await? {
            ReadResponse::AddressBalance { balance, .. } => Ok(balance),
            _ => Err(unexpected("address balance")),
        }
    }

    async fn address_utxos(
        &self,
        addresses: HashSet<transparent::Address>,
    ) -> Result<Vec<AddressUtxo>, BackendError> {
        match self.read(ReadRequest::UtxosByAddresses(addresses)).await? {
            ReadResponse::AddressUtxos(utxos) => Ok(utxos
                .utxos()
                .map(|(address, txid, location, output)| AddressUtxo {
                    address,
                    outpoint: transparent::OutPoint {
                        hash: *txid,
                        index: location.output_index().index(),
                    },
                    output: output.clone(),
                    height: location.height(),
                })
                .collect()),
            _ => Err(unexpected("address UTXO")),
        }
    }

    async fn mempool_transactions(&self) -> Result<Vec<UnminedTx>, BackendError> {
        let node = self.node()?.clone();
        let runtime = tokio::runtime::Handle::current();
        block_on_node(move || {
            runtime
                .block_on(node.mempool_transactions())
                .map_err(|error| format!("Zakura mempool query failed: {error}"))
        })
        .await
    }

    async fn submit_transaction(
        &self,
        transaction: Transaction,
    ) -> Result<transaction::Hash, BackendError> {
        let node = self.node()?.clone();
        let runtime = tokio::runtime::Handle::current();
        let submitted = tokio::task::spawn_blocking(move || {
            runtime
                .block_on(node.submit_transaction(transaction))
                .map_err(|error| error.to_string())
        })
        .await
        .map_err(|error| BackendError::Unavailable(format!("Zakura task failed: {error}")))?;
        submitted.map(|id| id.mined_id()).map_err(|error| {
            BackendError::Rejected(format!("Zakura rejected transaction: {error}"))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn errors_map_to_one_status_code_each() {
        assert_eq!(
            Status::from(BackendError::Unavailable("a".into())).code(),
            tonic::Code::Unavailable
        );
        assert_eq!(
            Status::from(BackendError::Rejected("b".into())).code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            Status::from(BackendError::Unexpected("c".into())).code(),
            tonic::Code::Internal
        );
        assert_eq!(Status::from(BackendError::Rejected("b".into())).message(), "b");
    }

    #[tokio::test]
    async fn read_only_backend_has_no_mempool() {
        let (_state, read, _tip, _change) = zakura_state::init(
            zakura_state::Config::ephemeral(),
            &Network::Mainnet,
            block::Height::MAX,
            0,
        )
        .await
        .expect("ephemeral state initializes");
        let backend = InProcessBackend::new(read);
        assert_eq!(backend.network(), Network::Mainnet);
        assert_eq!(backend.tip_height().await.unwrap(), None);
        let error = backend.mempool_transactions().await.unwrap_err();
        assert_eq!(
            error,
            BackendError::Unavailable("embedded Zakura node handle is unavailable".into())
        );
    }
}
