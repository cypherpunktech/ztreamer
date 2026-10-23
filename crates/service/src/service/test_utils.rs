//! Index and record builders shared by the `service` unit tests.

use zakura_chain::{block, transaction};
use ztreamer_indexer::{
    Digest,
    codec::{CompactBlockRecord, TreeSizes},
    index::{Index, IndexState},
    ingest::OrderedBuilder,
    types::{ParsedCompactBlock, RawIndexBlock},
};
use ztreamer_protocol::proto;

pub(crate) fn range(start: u32, end: u32) -> proto::BlockRange {
    proto::BlockRange {
        start: Some(proto::BlockId { height: u64::from(start), hash: Vec::new() }),
        end: Some(proto::BlockId { height: u64::from(end), hash: Vec::new() }),
        pool_types: Vec::new(),
    }
}

pub(crate) fn parsed(height: u32) -> ParsedCompactBlock {
    ParsedCompactBlock {
        height,
        hash: hash(height),
        previous_hash: height.checked_sub(1).map(hash).unwrap_or([0; 32]),
        time: height,
        transactions: Vec::new(),
        sapling_additions: 0,
        orchard_additions: 0,
        ironwood_additions: 0,
    }
}

pub(crate) fn raw_block(height: u32) -> RawIndexBlock {
    let mut bytes = vec![0; 140];
    bytes[4..36].copy_from_slice(&height.checked_sub(1).map(hash).unwrap_or([0; 32]));
    bytes[100..104].copy_from_slice(&height.to_le_bytes());
    bytes.extend_from_slice(&[0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
    RawIndexBlock {
        height: block::Height(height),
        hash: block::Hash(hash(height)),
        bytes,
        txids: vec![transaction::Hash(hash(height))],
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

pub(crate) fn hash(height: u32) -> Digest {
    let mut hash = [0; 32];
    hash[..4].copy_from_slice(&height.to_be_bytes());
    hash
}
