//! The values that a [`StateBackend`](crate::StateBackend) answers.

use std::sync::Arc;

use zakura_chain::{
    block::{self},
    ironwood, orchard, sapling,
    transaction::Transaction,
    transparent,
};
use ztreamer_snapshot::{manifest_entry::Frontiers, pool::PoolRoots};

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
