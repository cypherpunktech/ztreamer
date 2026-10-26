//! Conversion between [`ExportedRange`] and the Replicator `ManifestEntry` message.
//!
//! Hashes, digests and subtree roots are 32 bytes, in the byte order that the `range_meta` layout
//! uses. The conversion to the message cannot fail. The conversion from the message refuses
//! wrong lengths, numbers too large for their type, and unknown enum values.

use sapling_crypto::Node as SaplingNode;
use zakura_chain::{block, ironwood, orchard, subtree::NoteCommitmentSubtreeIndex};
use ztreamer_protocol::replicate;

use super::{
    BlobDigest, ExportedRange, Frontiers, ManifestEntry, SubtreeNode, SubtreeRoot, VerifyState,
};
use crate::codec::TreeSizes;

/// Why a Replicator `ManifestEntry` does not convert to an [`ExportedRange`]. Each variant names
/// the message field.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum WireError {
    /// A nested message is not set.
    #[error("{0} is missing")]
    Missing(&'static str),
    /// A byte field is not 32 bytes.
    #[error("{0} is not 32 bytes")]
    Length(&'static str),
    /// A number is too large for its type.
    #[error("{0} is out of range")]
    Overflow(&'static str),
    /// An enum value is unknown or unspecified. The second value is the one on the wire.
    #[error("{0} has no meaning for {1}")]
    Enum(&'static str, i32),
    /// The subtree root with this index is not a node of its pool.
    #[error("subtree root {0} is not a node of its pool")]
    SubtreeRoot(u16),
}

impl From<ExportedRange> for replicate::ManifestEntry {
    fn from(range: ExportedRange) -> Self {
        let ExportedRange { entry, state } = range;
        let TreeSizes { sapling, orchard, ironwood } = entry.end_tree_sizes;
        Self {
            range_version: entry.range_version.into(),
            start_height: entry.start_height,
            end_height: entry.end_height,
            end_hash: entry.end_hash.0.to_vec(),
            end_tree_sizes: Some(replicate::TreeSizes { sapling, orchard, ironwood }),
            frontiers: Some(replicate::Frontiers {
                sapling: entry.frontiers.sapling,
                orchard: entry.frontiers.orchard,
                ironwood: entry.frontiers.ironwood,
            }),
            subtree_roots: entry.subtree_roots.into_iter().map(SubtreeRoot::into_wire).collect(),
            blob_digest: entry.blob_digest.0.to_vec(),
            blob_len: entry.blob_len,
            epoch_end: entry.epoch_end,
            state: state.into_wire().into(),
        }
    }
}

impl TryFrom<replicate::ManifestEntry> for ExportedRange {
    type Error = WireError;

    fn try_from(entry: replicate::ManifestEntry) -> Result<Self, WireError> {
        let sizes = entry.end_tree_sizes.ok_or(WireError::Missing("end_tree_sizes"))?;
        let frontiers = entry.frontiers.ok_or(WireError::Missing("frontiers"))?;
        Ok(Self {
            entry: ManifestEntry {
                range_version: entry
                    .range_version
                    .try_into()
                    .map_err(|_| WireError::Overflow("range_version"))?,
                start_height: entry.start_height,
                end_height: entry.end_height,
                end_hash: block::Hash(fixed("end_hash", entry.end_hash)?),
                end_tree_sizes: TreeSizes {
                    sapling: sizes.sapling,
                    orchard: sizes.orchard,
                    ironwood: sizes.ironwood,
                },
                frontiers: Frontiers {
                    sapling: frontiers.sapling,
                    orchard: frontiers.orchard,
                    ironwood: frontiers.ironwood,
                },
                subtree_roots: entry
                    .subtree_roots
                    .into_iter()
                    .map(SubtreeRoot::try_from_wire)
                    .collect::<Result<_, _>>()?,
                blob_digest: BlobDigest(fixed("blob_digest", entry.blob_digest)?),
                blob_len: entry.blob_len,
                epoch_end: entry.epoch_end,
            },
            state: VerifyState::try_from_wire(entry.state)?,
        })
    }
}

impl SubtreeRoot {
    fn into_wire(self) -> replicate::SubtreeRoot {
        let (pool, root) = match self.root {
            SubtreeNode::Sapling(node) => (replicate::Pool::Sapling, node.to_bytes()),
            SubtreeNode::Orchard(node) => (replicate::Pool::Orchard, node.to_repr()),
            SubtreeNode::Ironwood(node) => (replicate::Pool::Ironwood, node.to_repr()),
        };
        replicate::SubtreeRoot {
            pool: pool.into(),
            index: self.index.0.into(),
            root: root.to_vec(),
        }
    }

    fn try_from_wire(root: replicate::SubtreeRoot) -> Result<Self, WireError> {
        let index =
            u16::try_from(root.index).map_err(|_| WireError::Overflow("subtree_roots.index"))?;
        let bytes = fixed("subtree_roots.root", root.root)?;
        let node = match replicate::Pool::try_from(root.pool) {
            Ok(replicate::Pool::Sapling) => {
                Option::from(SaplingNode::from_bytes(bytes)).map(SubtreeNode::Sapling)
            }
            Ok(replicate::Pool::Orchard) => {
                orchard::tree::Node::try_from(bytes).ok().map(SubtreeNode::Orchard)
            }
            Ok(replicate::Pool::Ironwood) => {
                ironwood::tree::Node::try_from(bytes).ok().map(SubtreeNode::Ironwood)
            }
            Ok(replicate::Pool::Unspecified) | Err(_) => {
                return Err(WireError::Enum("subtree_roots.pool", root.pool));
            }
        };
        Ok(Self {
            index: NoteCommitmentSubtreeIndex(index),
            root: node.ok_or(WireError::SubtreeRoot(index))?,
        })
    }
}

impl VerifyState {
    const fn into_wire(self) -> replicate::VerifyState {
        match self {
            Self::Unverified => replicate::VerifyState::Unverified,
            Self::ChainOk => replicate::VerifyState::ChainOk,
            Self::Verified => replicate::VerifyState::Verified,
            Self::Rejected => replicate::VerifyState::Rejected,
        }
    }

    fn try_from_wire(value: i32) -> Result<Self, WireError> {
        match replicate::VerifyState::try_from(value) {
            Ok(replicate::VerifyState::Unverified) => Ok(Self::Unverified),
            Ok(replicate::VerifyState::ChainOk) => Ok(Self::ChainOk),
            Ok(replicate::VerifyState::Verified) => Ok(Self::Verified),
            Ok(replicate::VerifyState::Rejected) => Ok(Self::Rejected),
            Ok(replicate::VerifyState::Unspecified) | Err(_) => {
                Err(WireError::Enum("state", value))
            }
        }
    }
}

fn fixed(field: &'static str, bytes: Vec<u8>) -> Result<[u8; 32], WireError> {
    bytes.try_into().map_err(|_| WireError::Length(field))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::range_meta::full_meta;

    fn exported() -> ExportedRange {
        let meta = full_meta(2_000);
        ExportedRange { entry: meta.entry, state: meta.state }
    }

    #[test]
    fn an_entry_round_trips_in_every_state() {
        for state in [
            VerifyState::Unverified,
            VerifyState::ChainOk,
            VerifyState::Verified,
            VerifyState::Rejected,
        ] {
            let range = ExportedRange { state, ..exported() };
            let wire = replicate::ManifestEntry::from(range.clone());
            assert_eq!(wire.end_hash.len(), 32);
            assert_eq!(ExportedRange::try_from(wire).unwrap(), range);
        }
    }

    #[test]
    fn a_bad_message_is_refused_with_its_field() {
        type Change = fn(&mut replicate::ManifestEntry);
        let cases: [(Change, WireError); 7] = [
            (|wire| wire.end_tree_sizes = None, WireError::Missing("end_tree_sizes")),
            (|wire| wire.end_hash.truncate(31), WireError::Length("end_hash")),
            (|wire| wire.range_version = 256, WireError::Overflow("range_version")),
            (
                |wire| wire.subtree_roots[0].index = 65_536,
                WireError::Overflow("subtree_roots.index"),
            ),
            (|wire| wire.subtree_roots[0].pool = 0, WireError::Enum("subtree_roots.pool", 0)),
            // All ones is above every pool's field modulus.
            (|wire| wire.subtree_roots[1].root = vec![0xff; 32], WireError::SubtreeRoot(0)),
            (|wire| wire.state = 9, WireError::Enum("state", 9)),
        ];
        for (change, error) in cases {
            let mut wire = replicate::ManifestEntry::from(exported());
            change(&mut wire);
            assert_eq!(ExportedRange::try_from(wire), Err(error));
        }
    }
}
