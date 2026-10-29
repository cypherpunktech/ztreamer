//! Index fixtures for the tests of a snapshot export.
//!
//! Compiled for this crate's own tests and for other crates that enable the `test-utils`
//! feature in their `[dev-dependencies]`.

use std::path::Path;

use zakura_chain::{orchard, parameters::Network, subtree::NoteCommitmentSubtreeIndex};
use ztreamer_chain::{codec::RANGE_SIZE, test_utils::record};
use ztreamer_index::{
    index::{
        Index,
        range_meta::{RangeMeta, VerifyState},
    },
    test_utils::{populate, sealed_range_blob},
};
use ztreamer_snapshot::{
    manifest_entry::{Frontiers, ManifestEntry},
    pool::{SubtreeNode, SubtreeRoot},
};

/// A mainnet index at `dir` with `ranges` sealed ranges and a `range_meta` row for each
/// `(start, epoch_end)` in `rows`. Returns the index and the entries of the rows.
pub fn index_with_rows(
    dir: &Path,
    ranges: u32,
    rows: &[(u32, bool)],
) -> (Index, Vec<ManifestEntry>) {
    let network = Network::Mainnet;
    let index =
        Index::open(dir, 512 * 1024 * 1024, &network.to_string(), network.genesis_hash()).unwrap();
    let records: Vec<_> =
        (0..ranges * RANGE_SIZE + 10).map(|height| record(height, Vec::new())).collect();
    populate(&index, &records, Some(ranges * RANGE_SIZE - 1));
    let entries = rows
        .iter()
        .map(|&(start, epoch_end)| {
            let blob = sealed_range_blob(&index, start);
            let range = &records[start as usize..][..RANGE_SIZE as usize];
            let entry = ManifestEntry {
                frontiers: Frontiers {
                    sapling: vec![0x01, 0xab, 0x00],
                    orchard: Vec::new(),
                    ironwood: Vec::new(),
                },
                subtree_roots: vec![SubtreeRoot {
                    index: NoteCommitmentSubtreeIndex(1),
                    root: SubtreeNode::Orchard(orchard::tree::Node::try_from([2; 32]).unwrap()),
                }],
                epoch_end,
                ..ManifestEntry::from_records_and_blob(range, &blob)
            };
            let meta = RangeMeta {
                entry: entry.clone(),
                state: VerifyState::Verified,
                verified_by: None,
                verified_at: None,
                attested: None,
                sampled_heights: Vec::new(),
            };
            index.put_range_meta(start, &meta).unwrap();
            entry
        })
        .collect();
    (index, entries)
}
