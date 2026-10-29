//! Index fixtures shared by tests and benchmarks: an index on a temporary path, and an index
//! with synthetic blocks.
//!
//! Compiled for this crate's own tests and for other crates that enable the `test-utils`
//! feature in their `[dev-dependencies]`. The synthetic blocks are in `ztreamer_chain::test_utils`.

use std::path::Path;

use zakura_chain::block;
use ztreamer_chain::{codec::CompactBlockRecord, test_utils::parsed};

use crate::{
    index::{Index, IndexState, range_meta::RangeMeta},
    ingest::OrderedBuilder,
};

/// Opens an index at `path` for mainnet with the genesis hash `[1; 32]` and a 64 MiB map.
///
/// For a test that needs another chain identity or map size, call [`Index::open`].
pub fn open_index(path: impl AsRef<Path>) -> Index {
    Index::open(path, 64 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap()
}

/// Reads the `range_meta` row of the sealed range that starts at `start`, at the current
/// generation.
///
/// # Panics
///
/// Panics if the read fails or the range has no row.
pub fn range_meta_row(index: &Index, start: u32) -> RangeMeta {
    index.latest().unwrap().range_meta(start).unwrap().unwrap()
}

/// Reads the packed blob of the sealed range that starts at `start`, at the current generation.
///
/// # Panics
///
/// Panics if the read fails or no sealed range starts at `start`.
pub fn sealed_range_blob(index: &Index, start: u32) -> Vec<u8> {
    index.latest().unwrap().sealed_range(start).unwrap().unwrap()
}

/// Writes `records` into `index` in batches and returns the resulting state.
pub fn populate(
    index: &Index,
    records: &[CompactBlockRecord],
    seal_through: Option<u32>,
) -> IndexState {
    let mut state = index.state().unwrap();
    let tip = records.last().unwrap().height;
    let mut builder = OrderedBuilder::new(state, 256 * 1024 * 1024).unwrap();
    for record in records {
        builder.push(parsed(record.clone())).unwrap();
    }
    while let Some(batch) = builder.build_batch(Some(tip), seal_through, 16 * 1024 * 1024).unwrap()
    {
        state = index.write(batch).unwrap();
    }
    assert_eq!(state.durable_tip().unwrap().height, tip);
    state
}
