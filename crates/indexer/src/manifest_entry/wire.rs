//! Conversion between [`ManifestEntry`] and the Replicator `ManifestEntry` message.
//!
//! Hashes, digests and subtree roots are 32 bytes, in the byte order that the `range_meta` layout
//! uses. The conversion to the message cannot fail. The conversion from the message refuses
//! wrong lengths, numbers too large for their type, and unknown enum values.

use zakura_chain::{block, subtree::NoteCommitmentSubtreeIndex};
use ztreamer_protocol::replicate;

use super::{BlobDigest, Frontiers, ManifestEntry};
use crate::pool::{NodeBytes, Pool, SubtreeRoot};
use ztreamer_chain::codec::TreeSizes;

/// Why a Replicator `ManifestEntry` does not convert to a [`ManifestEntry`]. Each variant names
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

impl From<ManifestEntry> for replicate::ManifestEntry {
    fn from(entry: ManifestEntry) -> Self {
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
            subtree_roots: entry
                .subtree_roots
                .into_iter()
                .map(replicate::SubtreeRoot::from)
                .collect(),
            blob_digest: entry.blob_digest.0.to_vec(),
            blob_len: entry.blob_len,
            epoch_end: entry.epoch_end,
        }
    }
}

impl TryFrom<replicate::ManifestEntry> for ManifestEntry {
    type Error = WireError;

    fn try_from(entry: replicate::ManifestEntry) -> Result<Self, WireError> {
        let sizes = entry.end_tree_sizes.ok_or(WireError::Missing("end_tree_sizes"))?;
        let frontiers = entry.frontiers.ok_or(WireError::Missing("frontiers"))?;
        Ok(Self {
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
                .map(SubtreeRoot::try_from)
                .collect::<Result<_, _>>()?,
            blob_digest: BlobDigest(fixed("blob_digest", entry.blob_digest)?),
            blob_len: entry.blob_len,
            epoch_end: entry.epoch_end,
        })
    }
}

impl From<SubtreeRoot> for replicate::SubtreeRoot {
    fn from(root: SubtreeRoot) -> Self {
        Self {
            pool: replicate::Pool::from(root.root.pool()).into(),
            index: root.index.0.into(),
            root: root.root.to_bytes().as_ref().to_vec(),
        }
    }
}

impl TryFrom<replicate::SubtreeRoot> for SubtreeRoot {
    type Error = WireError;

    fn try_from(root: replicate::SubtreeRoot) -> Result<Self, WireError> {
        let index =
            u16::try_from(root.index).map_err(|_| WireError::Overflow("subtree_roots.index"))?;
        let bytes =
            NodeBytes::try_from(root.root).map_err(|_| WireError::Length("subtree_roots.root"))?;
        let pool = replicate::Pool::try_from(root.pool)
            .map_err(|_| WireError::Enum("subtree_roots.pool", root.pool))?;
        let node = Pool::try_from(pool)?.node(bytes);
        Ok(Self {
            index: NoteCommitmentSubtreeIndex(index),
            root: node.ok_or(WireError::SubtreeRoot(index))?,
        })
    }
}

impl From<Pool> for replicate::Pool {
    fn from(pool: Pool) -> Self {
        match pool {
            Pool::Sapling => Self::Sapling,
            Pool::Orchard => Self::Orchard,
            Pool::Ironwood => Self::Ironwood,
        }
    }
}

impl TryFrom<replicate::Pool> for Pool {
    type Error = WireError;

    fn try_from(pool: replicate::Pool) -> Result<Self, WireError> {
        match pool {
            replicate::Pool::Sapling => Ok(Self::Sapling),
            replicate::Pool::Orchard => Ok(Self::Orchard),
            replicate::Pool::Ironwood => Ok(Self::Ironwood),
            replicate::Pool::Unspecified => Err(WireError::Enum("subtree_roots.pool", pool.into())),
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

    /// An entry with subtree roots of the Sapling, Orchard and Ironwood pools.
    fn entry() -> ManifestEntry {
        let mut entry = full_meta(2_000).entry;
        entry.subtree_roots.extend(full_meta(2_000).attested.unwrap().subtree_roots);
        entry
    }

    #[test]
    fn an_entry_round_trips() {
        let wire = replicate::ManifestEntry::from(entry());
        assert_eq!(wire.end_hash.len(), 32);
        let pools: Vec<_> = wire.subtree_roots.iter().map(|root| root.pool).collect();
        assert_eq!(pools, [1, 2, 3]);
        assert_eq!(ManifestEntry::try_from(wire).unwrap(), entry());
    }

    #[test]
    fn a_bad_message_is_refused_with_its_field() {
        type Change = fn(&mut replicate::ManifestEntry);
        let cases: [(Change, WireError); 6] = [
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
        ];
        for (change, error) in cases {
            let mut wire = replicate::ManifestEntry::from(entry());
            change(&mut wire);
            assert_eq!(ManifestEntry::try_from(wire), Err(error));
        }
    }
}
