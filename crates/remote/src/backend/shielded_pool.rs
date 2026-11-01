//! The shielded pools of `z_getsubtreesbyindex`.

use sapling_crypto::Node as SaplingNode;
use zakura_chain::{
    ironwood, orchard,
    subtree::{NoteCommitmentSubtreeData, NoteCommitmentSubtreeIndex},
};
use zakura_rpc::client::GetSubtreesByIndexResponse;
use ztreamer_backend::{BackendError, Subtrees};
use ztreamer_snapshot::pool::{self, NodeBytes, SubtreeNode};

/// A shielded pool: its [`pool::Pool`] and its root type.
pub(crate) trait Pool {
    /// The pool in the snapshot format.
    const POOL: pool::Pool;
    /// The name of the pool in `z_getsubtreesbyindex`.
    const NAME: &'static str = Self::POOL.as_str();
    /// The type of a subtree root in the pool.
    type Node: Clone + Send + Sync + 'static;

    /// Returns the root in `node`. Returns `None` if `node` is a root of a different pool.
    fn root_from_node(node: SubtreeNode) -> Option<Self::Node>;

    /// Parses a `z_getsubtreesbyindex` answer that must start at `start`.
    ///
    /// The node prints each subtree root in the stored byte order, for all pools. This is
    /// different from the Sapling tree root (`finalRoot` of `z_gettreestate`). The node prints
    /// that root in the reverse byte order.
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
                    .and_then(|bytes| Self::POOL.read_node(bytes))
                    .and_then(Self::root_from_node)
                    .ok_or_else(|| bad_root(&subtree.root))?;
                let data = NoteCommitmentSubtreeData::new(subtree.end_height, root);
                Ok((NoteCommitmentSubtreeIndex(index), data))
            })
            .collect()
    }
}

/// The Sapling pool.
pub(crate) enum SaplingPool {}

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

/// The Orchard pool.
pub(crate) enum OrchardPool {}

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

/// The Ironwood pool.
pub(crate) enum IronwoodPool {}

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
