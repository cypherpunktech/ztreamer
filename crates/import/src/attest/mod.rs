//! Attestation: the node confirms what an epoch rebuilt.
//!
//! [`EpochProof::rebuild`] proves that the ranges of an epoch agree with the manifest.
//! [`EpochProof::attest`] then asks an [`Attestor`] about the epoch: the block hash at the end of
//! each range, the three tree roots at the end of the epoch, once, and the roots of the subtrees
//! that the epoch completed, with one call per pool. When all answers agree, it returns one
//! [`Attested`] record per range, which the importer keeps in `range_meta`.
//!
//! [`JsonRpcAttestor`] asks a zakura node over JSON-RPC. `z_gettreestate` takes 0.2 to 2.6 s and
//! the node does not cache it, so the attestor keeps the tree roots that it gave: a retry of an
//! epoch makes no new `z_gettreestate` call. The other two calls take less than a millisecond.
//!
//! [`EpochProof::rebuild`]: crate::proof::EpochProof::rebuild
//! [`EpochProof::attest`]: crate::proof::EpochProof::attest
//! [`Attested`]: ztreamer_index::index::range_meta::Attested

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use async_trait::async_trait;
use tokio::sync::OnceCell;
use zakura_chain::{block, subtree::NoteCommitmentSubtreeIndex};
use zakura_rpc::client::Commitments;
use zakura_state::HashOrHeight;
use ztreamer_blocking::BlockingTaskCancelled;
use ztreamer_chain::{
    codec::CodecError,
    parser::CompactParseError,
    types::{BlockAtHeightError, RawBlockError},
};
use ztreamer_remote::{
    FetchAtHeightError, RpcError, ZakuraClient,
    frontier::{FromRpc, RpcDecodeError},
};
use ztreamer_snapshot::pool::{NodeBytes, Pool, PoolRoots, SubtreeNode};

use crate::range::Reject;

/// The most `z_gettreestate` answers that a [`JsonRpcAttestor`] keeps.
pub const CACHE_CAPACITY: usize = 1024;

/// A trusted source of the chain state at a height, usually a node.
///
/// # Implementation
///
/// Give the answers of the best chain. Return a [`NodeError`] when there is no answer, not a
/// default value: [`EpochProof::attest`](crate::proof::EpochProof::attest) rejects a range on an
/// answer that does not agree. A root is `None` when the pool is not active at the height. An
/// implementation may cache its answers, because the answers at a height below the tip do not
/// change.
#[async_trait]
pub trait Attestor: Send + Sync {
    /// The hash of the best-chain block at `height`.
    async fn block_hash(&self, height: u32) -> Result<block::Hash, NodeError>;

    /// The three note commitment tree roots after the block at `height`.
    async fn tree_roots(&self, height: u32) -> Result<PoolRoots, NodeError>;

    /// The roots of the `count` completed subtrees of `pool` from `start`, in index order.
    /// Give exactly `count` roots, or [`NodeError::MissingSubtree`] for the first that the
    /// source does not have.
    async fn subtree_roots(
        &self,
        pool: Pool,
        start: NoteCommitmentSubtreeIndex,
        count: u16,
    ) -> Result<Vec<SubtreeNode>, NodeError>;
}

/// Why a node call gave no answer.
#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    /// The call to the node failed.
    #[error(transparent)]
    Rpc(#[from] RpcError),
    /// The node answered for another height.
    #[error("node answered for height {actual}, not {expected}")]
    Height {
        /// The height that the call asked for.
        expected: u32,
        /// The height in the answer.
        actual: u32,
    },
    /// A `finalRoot` of `z_gettreestate` does not parse.
    #[error("{pool:?} final root does not parse")]
    Root {
        /// The pool of the root.
        pool: Pool,
        /// Why the root does not parse.
        #[source]
        source: RpcDecodeError,
    },
    /// `z_gettreestate` gives a `finalState` but no `finalRoot` for the pool. The pool is
    /// active, thus the answer has no root to compare.
    #[error("{pool:?} tree state has no final root")]
    MissingRoot {
        /// The pool of the tree state.
        pool: Pool,
    },
    /// The node does not have the subtree.
    #[error("node has no {pool:?} subtree {}", .index.0)]
    MissingSubtree {
        /// The pool of the subtree.
        pool: Pool,
        /// The index of the subtree.
        index: NoteCommitmentSubtreeIndex,
    },
    /// The subtree root is not the hex of a node of the pool.
    #[error("{pool:?} subtree root {} does not parse", .index.0)]
    SubtreeRoot {
        /// The pool of the subtree.
        pool: Pool,
        /// The index of the subtree.
        index: NoteCommitmentSubtreeIndex,
    },
    /// The node block does not deserialize, or has no coinbase height.
    #[error(transparent)]
    RawBlock(#[from] RawBlockError),
    /// The indexer parser does not read the node block.
    #[error(transparent)]
    Parse(#[from] CompactParseError),
    /// The record of the node block does not encode.
    #[error("record of the node block does not encode")]
    Encode(#[from] CodecError),
    /// The blocking pool stopped before the local work on the answer ran. The runtime stops;
    /// the node is not at fault. The importer gives
    /// [`ImportError::Cancelled`](crate::importer::ImportError::Cancelled) for it.
    #[error(transparent)]
    Cancelled(#[from] BlockingTaskCancelled),
}

impl From<BlockAtHeightError> for NodeError {
    fn from(error: BlockAtHeightError) -> Self {
        match error {
            BlockAtHeightError::Decode { source, .. } => Self::RawBlock(source),
            BlockAtHeightError::Height { expected, actual } => Self::Height { expected, actual },
        }
    }
}

impl From<FetchAtHeightError> for NodeError {
    fn from(error: FetchAtHeightError) -> Self {
        match error {
            FetchAtHeightError::Rpc(error) => Self::Rpc(error),
            FetchAtHeightError::Block(error) => error.into(),
            FetchAtHeightError::Cancelled(error) => Self::Cancelled(error),
        }
    }
}

/// Why [`EpochProof::attest`](crate::proof::EpochProof::attest) or
/// [`SamplePlan::sample`](crate::sample::SamplePlan::sample) did not accept an epoch.
#[derive(Debug, thiserror::Error)]
pub enum EpochError {
    /// A range of the epoch is wrong, or a blob must be fetched again.
    #[error(transparent)]
    Reject(#[from] Reject),
    /// The node gave no answer. This says nothing about the ranges: try again later.
    #[error("node gave no answer for height {height}")]
    Node {
        /// The height that the failed call asked about.
        height: u32,
        /// Why the node gave no answer.
        #[source]
        source: NodeError,
    },
}

/// An [`Attestor`] that asks a zakura node over JSON-RPC, and keeps the tree roots.
///
/// It keeps up to [`CACHE_CAPACITY`] `z_gettreestate` answers. When the map is full, it drops
/// the answer for the lowest height. Calls for one height at the same time make one
/// `z_gettreestate` call.
///
/// ```no_run
/// use ztreamer_remote::{RpcClient, ZakuraClient};
/// use ztreamer_import::attest::{Attestor, JsonRpcAttestor};
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let config = ZakuraClient::config("http://127.0.0.1:8232".parse()?);
/// let attestor = JsonRpcAttestor::new(ZakuraClient::new(RpcClient::new(config)?));
/// let roots = attestor.tree_roots(3_524_999).await?;
/// # let _ = roots;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct JsonRpcAttestor {
    node: ZakuraClient,
    /// The tree roots by height. A cell is empty until a call for its height succeeds. The map
    /// drops its lowest height when it is full.
    roots: Mutex<BTreeMap<u32, Arc<OnceCell<PoolRoots>>>>,
}

impl JsonRpcAttestor {
    /// An attestor with no answers yet.
    pub const fn new(node: ZakuraClient) -> Self {
        Self { node, roots: Mutex::new(BTreeMap::new()) }
    }

    /// The map of the tree roots. The map is valid after a panic, because no change leaves it
    /// half done.
    fn lock_roots(&self) -> MutexGuard<'_, BTreeMap<u32, Arc<OnceCell<PoolRoots>>>> {
        self.roots.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The cell of the tree roots at `height`. A new cell is empty. Before it adds a cell to a
    /// full map, it drops the filled cells of the lowest heights. It never drops a cell that a
    /// call still fills.
    fn tree_roots_cell(&self, height: u32) -> Arc<OnceCell<PoolRoots>> {
        let mut map = self.lock_roots();
        if let Some(cell) = map.get(&height) {
            return Arc::clone(cell);
        }
        while map.len() >= CACHE_CAPACITY {
            let Some(lowest) = map
                .iter()
                .find(|(_, cell)| cell.initialized())
                .map(|(&kept_height, _)| kept_height)
            else {
                break;
            };
            map.remove(&lowest);
        }
        Arc::clone(map.entry(height).or_default())
    }

    /// Removes the cell of a failed call at `height`, when the cell is still empty and no other
    /// call holds it. Thus a failed call keeps no place in the map.
    fn forget_failed_cell(&self, height: u32, cell: &Arc<OnceCell<PoolRoots>>) {
        let mut map = self.lock_roots();
        // The map and `cell` are the two holders. Another call gets a holder only under the lock.
        let unused = map.get(&height).is_some_and(|kept| {
            Arc::ptr_eq(kept, cell) && !kept.initialized() && Arc::strong_count(kept) == 2
        });
        if unused {
            map.remove(&height);
        }
    }

    /// Asks the node for the tree roots at `height` with `z_gettreestate`.
    async fn ask_tree_roots(&self, height: u32) -> Result<PoolRoots, NodeError> {
        let state = self.node.z_get_treestate(HashOrHeight::Height(block::Height(height))).await?;
        let actual = state.height().0;
        if actual != height {
            return Err(NodeError::Height { expected: height, actual });
        }
        Ok(PoolRoots {
            sapling: final_root(Pool::Sapling, state.sapling().commitments())?,
            orchard: final_root(Pool::Orchard, state.orchard().commitments())?,
            ironwood: final_root(Pool::Ironwood, state.ironwood().commitments())?,
        })
    }
}

#[async_trait]
impl Attestor for JsonRpcAttestor {
    async fn block_hash(&self, height: u32) -> Result<block::Hash, NodeError> {
        Ok(self.node.get_block_hash(block::Height(height)).await?.hash())
    }

    async fn tree_roots(&self, height: u32) -> Result<PoolRoots, NodeError> {
        let cell = self.tree_roots_cell(height);
        let roots = cell.get_or_try_init(|| self.ask_tree_roots(height)).await.copied();
        if roots.is_err() {
            self.forget_failed_cell(height, &cell);
        }
        roots
    }

    async fn subtree_roots(
        &self,
        pool: Pool,
        start: NoteCommitmentSubtreeIndex,
        count: u16,
    ) -> Result<Vec<SubtreeNode>, NodeError> {
        let indices = (0..count)
            .map(|offset| start.0.checked_add(offset).map(NoteCommitmentSubtreeIndex))
            .collect::<Option<Vec<_>>>()
            .ok_or(NodeError::MissingSubtree { pool, index: start })?;
        let limit = Some(NoteCommitmentSubtreeIndex(count));
        let response = self.node.z_get_subtrees_by_index(pool.as_str(), start, limit).await?;
        if response.start_index() != start {
            return Err(NodeError::MissingSubtree { pool, index: start });
        }
        indices
            .into_iter()
            .enumerate()
            .map(|(offset, index)| {
                let subtree = response
                    .subtrees()
                    .get(offset)
                    .ok_or(NodeError::MissingSubtree { pool, index })?;
                subtree
                    .root
                    .parse::<NodeBytes>()
                    .ok()
                    .and_then(|bytes| pool.node(bytes))
                    .ok_or(NodeError::SubtreeRoot { pool, index })
            })
            .collect()
    }
}

/// Reads the `finalRoot` of `pool` in the byte order of the pool. See
/// [`ztreamer_remote::frontier`]. A pool with no `finalState` is not active, and has no root.
/// It is a free function because it is generic over the root type of the pool.
fn final_root<R: FromRpc>(pool: Pool, commitments: &Commitments) -> Result<Option<R>, NodeError> {
    match (commitments.final_root(), commitments.final_state()) {
        (Some(root), _) => {
            R::from_rpc_bytes(root).map(Some).map_err(|source| NodeError::Root { pool, source })
        }
        (None, Some(_)) => Err(NodeError::MissingRoot { pool }),
        (None, None) => Ok(None),
    }
}

#[cfg(test)]
mod tests;
