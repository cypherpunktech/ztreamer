//! Manifest entries shared by tests and benchmarks.
//!
//! Compiled for this crate's own tests and for other crates that enable the `test-utils`
//! feature in their `[dev-dependencies]`.

use sapling_crypto::Node as SaplingNode;
use zakura_chain::{orchard, subtree::NoteCommitmentSubtreeIndex};
use ztreamer_chain::{
    codec::{RANGE_SIZE, TreeSizes},
    test_utils::hash,
};

use crate::{
    epoch::Epochs,
    manifest_entry::{BlobDigest, Frontiers, ManifestEntry},
    pool::{SubtreeNode, SubtreeRoot},
};

/// Contiguous entries from genesis. Entry `i` adds `adds[i]` commitments to the trees, and the
/// `epoch_end` flags agree with [`Epochs`].
pub fn synthetic_entries(adds: &[TreeSizes]) -> Vec<ManifestEntry> {
    let mut sizes = TreeSizes::default();
    let mut entries: Vec<_> = (0..)
        .zip(adds)
        .map(|(index, add)| {
            sizes = sizes.checked_add(*add).unwrap();
            let start_height = index * RANGE_SIZE;
            let end_height = start_height + RANGE_SIZE - 1;
            ManifestEntry {
                range_version: 1,
                start_height,
                end_height,
                end_hash: hash(end_height),
                end_tree_sizes: sizes,
                frontiers: Frontiers::default(),
                subtree_roots: Vec::new(),
                blob_digest: BlobDigest([0; 32]),
                blob_len: 0,
                epoch_end: false,
            }
        })
        .collect();
    let closes: Vec<_> = Epochs::new(&entries)
        .scan(0, |end, epoch| {
            *end += epoch.entries().len();
            Some(*end - 1)
        })
        .collect();
    for index in closes {
        entries[index].epoch_end = true;
    }
    entries
}

/// The entry of the range at `start` with every frontier and two subtree roots, of Sapling and
/// Orchard.
pub fn full_entry(start: u32) -> ManifestEntry {
    ManifestEntry {
        range_version: 1,
        start_height: start,
        end_height: start + 999,
        end_hash: hash(start + 999),
        end_tree_sizes: TreeSizes { sapling: 61_231_877, orchard: 12, ironwood: 0 },
        frontiers: Frontiers {
            sapling: vec![0xab; 1_100],
            orchard: vec![0xcd; 900],
            ironwood: Vec::new(),
        },
        subtree_roots: vec![
            SubtreeRoot {
                index: NoteCommitmentSubtreeIndex(934),
                root: SubtreeNode::Sapling(SaplingNode::from_bytes([1; 32]).unwrap()),
            },
            SubtreeRoot {
                index: NoteCommitmentSubtreeIndex(0),
                root: SubtreeNode::Orchard(orchard::tree::Node::try_from([2; 32]).unwrap()),
            },
        ],
        blob_digest: BlobDigest([7; 32]),
        blob_len: 121_034_512,
        epoch_end: true,
    }
}
