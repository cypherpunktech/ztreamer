//! Range and record builders shared by the `service` unit tests.

use ztreamer_indexer::{
    codec::{CompactBlockRecord, TreeSizes},
    test_utils,
    types::ParsedCompactBlock,
};

pub(crate) use crate::test_utils::range;
pub(crate) use ztreamer_indexer::test_utils::{hash, raw_block};

/// An empty block at `height` that chains onto [`hash`] of the previous height.
pub(crate) fn parsed(height: u32) -> ParsedCompactBlock {
    test_utils::parsed(test_utils::record(height, Vec::new()))
}

pub(crate) fn record(height: u32) -> CompactBlockRecord {
    CompactBlockRecord {
        height,
        hash: hash(height),
        previous_hash: hash(height - 1),
        time: height,
        transactions: Vec::new(),
        end_tree_sizes: TreeSizes::default(),
    }
}
