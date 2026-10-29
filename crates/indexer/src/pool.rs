//! The note commitment pools, and the roots of their subtrees and trees.

use std::{array::TryFromSliceError, str::FromStr};

use hex::{FromHex, FromHexError};
use sapling_crypto::Node as SaplingNode;
use zakura_chain::{ironwood, orchard, sapling, subtree::NoteCommitmentSubtreeIndex};

use ztreamer_chain::codec::{CodecError, Reader, put_count};

/// A note commitment pool.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Pool {
    /// The Sapling pool.
    Sapling,
    /// The Orchard pool.
    Orchard,
    /// The Ironwood pool.
    Ironwood,
}

impl Pool {
    /// The three pools.
    pub const ALL: [Self; 3] = [Self::Sapling, Self::Orchard, Self::Ironwood];

    /// The name of the pool in a snapshot manifest and in `z_getsubtreesbyindex`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sapling => "sapling",
            Self::Orchard => "orchard",
            Self::Ironwood => "ironwood",
        }
    }

    /// Reads a subtree root of this pool from `bytes`. Returns `None` if the bytes are not a
    /// node of the pool.
    pub fn node(self, NodeBytes(bytes): NodeBytes) -> Option<SubtreeNode> {
        match self {
            Self::Sapling => Option::from(SaplingNode::from_bytes(bytes)).map(SubtreeNode::Sapling),
            Self::Orchard => orchard::tree::Node::try_from(bytes).ok().map(SubtreeNode::Orchard),
            Self::Ironwood => ironwood::tree::Node::try_from(bytes).ok().map(SubtreeNode::Ironwood),
        }
    }

    /// The tag of the pool in the `range_meta` layout.
    const fn tag(self) -> u8 {
        match self {
            Self::Sapling => 0,
            Self::Orchard => 1,
            Self::Ironwood => 2,
        }
    }

    const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0 => Some(Self::Sapling),
            1 => Some(Self::Orchard),
            2 => Some(Self::Ironwood),
            _ => None,
        }
    }
}

/// The root node of a completed note commitment subtree, in its pool's node type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubtreeNode {
    /// A Sapling subtree root.
    Sapling(SaplingNode),
    /// An Orchard subtree root.
    Orchard(orchard::tree::Node),
    /// An Ironwood subtree root.
    Ironwood(ironwood::tree::Node),
}

impl SubtreeNode {
    /// The pool of the subtree root.
    pub const fn pool(self) -> Pool {
        match self {
            Self::Sapling(_) => Pool::Sapling,
            Self::Orchard(_) => Pool::Orchard,
            Self::Ironwood(_) => Pool::Ironwood,
        }
    }

    /// The bytes of the subtree root. [`Pool::node`] reads them.
    pub fn to_bytes(self) -> NodeBytes {
        NodeBytes(match self {
            Self::Sapling(node) => node.to_bytes(),
            Self::Orchard(node) => node.to_repr(),
            Self::Ironwood(node) => node.to_repr(),
        })
    }

    fn encode_into(self, bytes: &mut Vec<u8>) {
        bytes.push(self.pool().tag());
        bytes.extend_from_slice(self.to_bytes().as_ref());
    }

    fn decode_from(reader: &mut Reader<'_>) -> Result<Self, CodecError> {
        let tag = reader.u8()?;
        let bytes = NodeBytes(reader.array()?);
        Pool::from_tag(tag).and_then(|pool| pool.node(bytes)).ok_or(CodecError::InvalidRecord)
    }
}

/// A subtree root as 32 bytes, in the byte order of its pool. `z_getsubtreesbyindex`, the
/// `range_meta` layout, the Replicator message and the JSON manifest use this order.
///
/// The bytes are not checked. [`Pool::node`] checks them and returns a [`SubtreeNode`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodeBytes([u8; 32]);

impl AsRef<[u8]> for NodeBytes {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl TryFrom<&[u8]> for NodeBytes {
    type Error = TryFromSliceError;

    /// Refuses a slice that is not 32 bytes.
    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        bytes.try_into().map(Self)
    }
}

impl TryFrom<Vec<u8>> for NodeBytes {
    type Error = TryFromSliceError;

    /// Refuses a vector that is not 32 bytes.
    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        Self::try_from(bytes.as_slice())
    }
}

impl FromStr for NodeBytes {
    type Err = FromHexError;

    /// Reads 64 hex digits, as `z_getsubtreesbyindex` prints a subtree root.
    fn from_str(hex: &str) -> Result<Self, Self::Err> {
        <[u8; 32]>::from_hex(hex).map(Self)
    }
}

/// The root of one completed note commitment subtree.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubtreeRoot {
    /// The subtree's index in its pool's tree.
    pub index: NoteCommitmentSubtreeIndex,
    /// The subtree root. Its variant names the pool.
    pub root: SubtreeNode,
}

/// The tree roots a node reported for one height. A root is `None` when the pool was not active
/// at that height.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PoolRoots {
    /// Sapling root.
    pub sapling: Option<sapling::tree::Root>,
    /// Orchard root.
    pub orchard: Option<orchard::tree::Root>,
    /// Ironwood root.
    pub ironwood: Option<ironwood::tree::Root>,
}

pub(crate) fn put_subtree_roots(bytes: &mut Vec<u8>, roots: &[SubtreeRoot]) {
    put_count(bytes, roots.len());
    for root in roots {
        bytes.extend_from_slice(&root.index.0.to_be_bytes());
        root.root.encode_into(bytes);
    }
}

pub(crate) fn take_subtree_roots(reader: &mut Reader<'_>) -> Result<Vec<SubtreeRoot>, CodecError> {
    let count = reader.count()?;
    (0..count)
        .map(|_| {
            Ok(SubtreeRoot {
                index: NoteCommitmentSubtreeIndex(reader.u16()?),
                root: SubtreeNode::decode_from(reader)?,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A subtree root is a pool tag and 32 bytes. The decoder refuses an unknown tag and bytes
    /// that are not a field element of the pool.
    #[test]
    fn subtree_nodes_round_trip_and_reject_bad_bytes() {
        let nodes = [
            SubtreeNode::Sapling(SaplingNode::from_bytes([1; 32]).unwrap()),
            SubtreeNode::Orchard(orchard::tree::Node::try_from([2; 32]).unwrap()),
            SubtreeNode::Ironwood(ironwood::tree::Node::try_from([3; 32]).unwrap()),
        ];
        for (tag, node) in nodes.into_iter().enumerate() {
            let mut bytes = Vec::new();
            node.encode_into(&mut bytes);
            assert_eq!(bytes[0], tag as u8);
            assert_eq!(bytes.len(), 33);
            assert_eq!(SubtreeNode::decode_from(&mut Reader::new(&bytes)).unwrap(), node);

            let mut unknown_tag = bytes.clone();
            unknown_tag[0] = 3;
            assert_eq!(
                SubtreeNode::decode_from(&mut Reader::new(&unknown_tag)),
                Err(CodecError::InvalidRecord)
            );
            // All ones is above every pool's field modulus.
            let not_a_field_element: Vec<u8> = [bytes[0]].into_iter().chain([0xff; 32]).collect();
            assert_eq!(
                SubtreeNode::decode_from(&mut Reader::new(&not_a_field_element)),
                Err(CodecError::InvalidRecord)
            );
        }
    }

    #[test]
    fn node_bytes_are_32_bytes_or_64_hex_digits() {
        let bytes = NodeBytes([0xab; 32]);
        assert_eq!("ab".repeat(32).parse(), Ok(bytes));
        assert_eq!(NodeBytes::try_from(vec![0xab; 32]).unwrap(), bytes);
        assert!("ab".repeat(31).parse::<NodeBytes>().is_err());
        assert!("zz".repeat(32).parse::<NodeBytes>().is_err());
        assert!(NodeBytes::try_from(vec![0xab; 33]).is_err());
    }
}
