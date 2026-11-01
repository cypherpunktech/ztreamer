//! [`StateBackend`]: the chain-state reads that ztreamer needs from a node, and the values that
//! they return.

use std::{
    collections::{BTreeMap, HashSet},
    ops::RangeInclusive,
    sync::Arc,
};

use sapling_crypto::Node as SaplingNode;
use zakura_chain::{
    amount::{Amount, NonNegative},
    block::{self, Block},
    ironwood, orchard,
    parameters::Network,
    sapling,
    subtree::{NoteCommitmentSubtreeData, NoteCommitmentSubtreeIndex},
    transaction::{self, Transaction, UnminedTx},
    transparent,
};
use ztreamer_snapshot::{manifest_entry::Frontiers, pool::PoolRoots};

use crate::BackendError;

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

/// Completed note commitment subtrees of one pool, by subtree index.
pub type Subtrees<Node> = BTreeMap<NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<Node>>;

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
