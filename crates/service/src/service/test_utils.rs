//! Index and record builders shared by the `service` unit tests.

use ztreamer_indexer::{
    codec::{CompactBlockRecord, TreeSizes},
    index::{Index, IndexState},
    ingest::OrderedBuilder,
};
use ztreamer_protocol::proto;

pub(crate) use ztreamer_indexer::test_utils::{hash, prepared as parsed, raw_block};

pub(crate) fn range(start: u32, end: u32) -> proto::BlockRange {
    proto::BlockRange {
        start: Some(proto::BlockId { height: u64::from(start), hash: Vec::new() }),
        end: Some(proto::BlockId { height: u64::from(end), hash: Vec::new() }),
        pool_types: Vec::new(),
    }
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

pub(crate) fn index_through(index: &Index, tip: u32) -> IndexState {
    let mut builder = OrderedBuilder::new(IndexState::default(), 1024 * 1024).unwrap();
    for height in 0..=tip {
        builder.push(parsed(height)).unwrap();
    }
    let mut state = IndexState::default();
    while let Some(batch) = builder.build_batch(Some(tip), Some(tip), 1024 * 1024).unwrap() {
        state = index.write(batch).unwrap();
    }
    state
}
