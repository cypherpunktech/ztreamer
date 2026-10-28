//! Block batches and branches for the tests of the index modules.

use zakura_chain::block;

use super::{BlockId, Index, PERSIST_DEPTH, SEAL_DEPTH};
use crate::{
    codec::{CompactBlockRecord, TreeSizes},
    ingest::{OrderedBuilder, WriteBatch},
    test_utils::{hash, parsed, record},
};

pub(crate) fn batch(
    index: &Index,
    heights: std::ops::RangeInclusive<u32>,
    source_tip: u32,
) -> WriteBatch {
    let mut builder = OrderedBuilder::new(index.state().unwrap(), 10 * 1024 * 1024).unwrap();
    for height in heights {
        builder.push(parsed(record(height, Vec::new()))).unwrap();
    }
    builder
        .build_batch(
            source_tip.checked_sub(PERSIST_DEPTH),
            source_tip.checked_sub(SEAL_DEPTH),
            10 * 1024 * 1024,
        )
        .unwrap()
        .unwrap()
}

/// A branch of empty blocks over `heights` that chains onto `ancestor`.
pub(crate) fn branch(
    ancestor: BlockId,
    heights: std::ops::RangeInclusive<u32>,
) -> Vec<CompactBlockRecord> {
    let mut previous_hash = ancestor.hash;
    heights
        .map(|height| {
            let record = CompactBlockRecord {
                height,
                hash: branch_hash(height),
                previous_hash,
                time: height,
                transactions: Vec::new(),
                end_tree_sizes: TreeSizes::default(),
            };
            previous_hash = record.hash;
            record
        })
        .collect()
}

pub(crate) fn branch_hash(height: u32) -> block::Hash {
    let mut hash = hash(height);
    hash.0[31] = 1;
    hash
}
