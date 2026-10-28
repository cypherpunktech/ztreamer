//! The chain-state seam: everything [`CompactService`](crate::CompactService) needs that is not
//! in the compact index.
//!
//! The compact index answers block and range requests. Everything else, from transparent address
//! lookups to tree states to the mempool, comes from a node. `ztreamer_node::InProcessBackend`
//! reaches the embedded Zakura in-process, through its `ReadStateService` and the mempool handle.
//! [`RemoteBackend`](crate::RemoteBackend) reaches a remote node over JSON-RPC.
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
use ztreamer_indexer::index::range_meta::{Frontiers, PoolRoots};

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

/// Each frontier in the `zcashd` RPC encoding, or empty when the pool is not active.
impl From<&TreeState> for Frontiers {
    fn from(trees: &TreeState) -> Self {
        Self {
            sapling: trees.sapling.as_ref().map(|tree| tree.to_rpc_bytes()).unwrap_or_default(),
            orchard: trees.orchard.as_ref().map(|tree| tree.to_rpc_bytes()).unwrap_or_default(),
            ironwood: trees.ironwood.as_ref().map(|tree| tree.to_rpc_bytes()).unwrap_or_default(),
        }
    }
}

/// Each tree root, or `None` when the pool is not active.
impl From<&TreeState> for PoolRoots {
    fn from(trees: &TreeState) -> Self {
        Self {
            sapling: trees.sapling.as_ref().map(|tree| tree.root()),
            orchard: trees.orchard.as_ref().map(|tree| tree.root()),
            ironwood: trees.ironwood.as_ref().map(|tree| tree.root()),
        }
    }
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
}
