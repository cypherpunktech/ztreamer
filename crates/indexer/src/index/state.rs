//! The state and the identity of an index: [`IndexState`], [`BlockId`] and [`ChainIdentity`].

use zakura_chain::{block, parameters::GENESIS_PREVIOUS_BLOCK_HASH};

use super::IndexError;
use ztreamer_chain::codec::TreeSizes;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// References a block by hash and height.
pub struct BlockId {
    pub height: u32,
    pub hash: block::Hash,
}

impl BlockId {
    pub const fn new(height: u32, hash: block::Hash) -> Self {
        Self { height, hash }
    }
}

/// The chain of an index, as its identity row stores it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChainIdentity {
    /// The network name that the index was opened with, for example `Mainnet`.
    pub network: String,
    /// The hash of the genesis block.
    pub genesis_hash: block::Hash,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
/// Represents the [`Index`](super::Index)'s current sync state.
pub struct IndexState {
    /// The highest contiguous compact block committed to the `mutable_blocks` database.
    pub(crate) durable_tip: Option<BlockId>,
    /// The highest height committed to the `sealed_ranges` database.
    ///
    /// Blocks above this height live in the `mutable_blocks` database.
    pub(crate) sealed_through: Option<u32>,
    /// The highest height through which every sealed range is verified against a node.
    ///
    /// Embedded mode never sets it. It is at most `sealed_through`.
    pub(crate) verified_through: Option<u32>,
    /// LMDB's monotonically increasing revision number.
    /// Incremented on each successful atomic index mutation.
    pub(crate) generation: u64,
    /// Cumulative [`TreeSizes`] at the end of `durable_tip`
    pub(crate) tree_sizes: TreeSizes,
}

impl IndexState {
    /// Returns the highest contiguous compact block committed to the
    /// `mutable_blocks` database.
    pub const fn durable_tip(&self) -> Option<BlockId> {
        self.durable_tip
    }

    /// Returns the highest height committed to the `sealed_ranges` database.
    ///
    /// Blocks above this height live in the `mutable_blocks` database.
    pub const fn sealed_through(&self) -> Option<u32> {
        self.sealed_through
    }

    /// Returns the highest height through which every sealed range is verified against a node,
    /// or `None` when no range is.
    pub const fn verified_through(&self) -> Option<u32> {
        self.verified_through
    }

    /// Identifies the exact committed revision of the durable index that an
    /// [`IndexState`] describes.
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Cumulative [`TreeSizes`] at the end of [`IndexState::durable_tip`]
    pub const fn tree_sizes(&self) -> TreeSizes {
        self.tree_sizes
    }

    /// Returns the first height of the next range to seal: `sealed_through + 1`, or 0 if no range
    /// is sealed.
    pub(crate) fn next_range_start(&self) -> Result<u32, IndexError> {
        self.sealed_through
            .map_or(Ok(0), |height| height.checked_add(1).ok_or(IndexError::Overflow))
    }

    /// Returns the first height of the next range to verify: `verified_through + 1`, or 0 if no
    /// range is verified.
    pub(crate) fn next_verify_start(&self) -> Result<u32, IndexError> {
        self.verified_through
            .map_or(Ok(0), |height| height.checked_add(1).ok_or(IndexError::Overflow))
    }

    /// The hash that the first block of an installed range links to: the hash of the tip, or the
    /// genesis previous hash on an empty index. An install requires no mutable rows. Thus the
    /// tip is the last block of the sealed ranges.
    pub(crate) fn install_parent_hash(&self) -> block::Hash {
        self.durable_tip.map_or(GENESIS_PREVIOUS_BLOCK_HASH, |tip| tip.hash)
    }

    /// Returns `true` if the index has mutable rows. The mutable rows cover the heights above
    /// `sealed_through` to the durable tip. Thus they exist only if the tip is above
    /// `sealed_through`.
    pub(crate) fn has_mutable_rows(&self) -> bool {
        self.durable_tip.map(|tip| tip.height) != self.sealed_through
    }
}
