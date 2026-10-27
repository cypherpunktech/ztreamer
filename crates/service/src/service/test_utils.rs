//! Range and record builders shared by the `service` unit tests.

use ztreamer_indexer::codec::{CompactBlockRecord, TreeSizes};

pub(crate) use crate::test_utils::range;
pub(crate) use ztreamer_indexer::test_utils::{hash, prepared as parsed, raw_block};

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
