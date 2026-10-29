//! [`StateBackend`]: the chain-state reads that ztreamer needs from a node.

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
    subtree::{NoteCommitmentSubtreeData, NoteCommitmentSubtreeIndex},
    transaction::{self, Transaction, UnminedTx},
    transparent,
};

use crate::{AddressUtxo, BackendError, MinedTransaction, TreeState};

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
