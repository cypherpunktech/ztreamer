//! The note commitment trees of a `z_gettreestate` answer.

use std::sync::Arc;

use crate::frontier::FromRpc;
use zakura_chain::{orchard, sapling};
use zakura_rpc::client::Treestate;
use ztreamer_backend::BackendError;

/// A note commitment tree that `z_gettreestate` prints, and the type of its root.
pub(crate) trait RpcTree: FromRpc {
    /// The type of the tree root.
    type Root: FromRpc + PartialEq;

    /// Returns the root of the tree.
    fn tree_root(&self) -> Self::Root;

    /// Reads the tree from `finalState` of one pool of a `z_gettreestate` answer. Returns
    /// `None` if `finalState` is not in the answer. The node does not send it before the pool
    /// activation. If the node also sends `finalRoot`, the tree root must be equal to it.
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
