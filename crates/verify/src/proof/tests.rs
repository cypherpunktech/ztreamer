use incrementalmerkletree::{Position, frontier::Frontier};
use zakura_chain::{subtree::NoteCommitmentSubtreeIndex, transaction};
use ztreamer_chain::{
    codec::{RANGE_SIZE, encode_range},
    test_utils::{hash, record},
    types::{CompactSaplingOutput, CompactTransaction},
};
use ztreamer_snapshot::manifest_entry::ManifestEntry;

use super::*;
use ztreamer_snapshot::{epoch::Epochs, test_utils::synthetic_entries};

/// The Sapling tree size before the epoch under test: six commitments short of the end of
/// subtree 15.
const START_SAPLING: u32 = 16 * 65_536 - 6;

/// A Sapling tree of [`START_SAPLING`] leaves with made-up nodes. Another `seed` gives other
/// nodes at the same size.
fn start_tree(seed: u8) -> sapling::tree::NoteCommitmentTree {
    let node = |byte: u8| sapling_crypto::Node::from_bytes([byte; 32]).unwrap();
    let position = Position::from(u64::from(START_SAPLING) - 1);
    let ommers = (0..position.past_ommer_count()).map(|i| node(seed + i)).collect();
    let frontier = Frontier::from_parts(position, node(seed), ommers).unwrap();
    sapling::tree::NoteCommitmentTree::from_frontier(frontier)
}

fn cmu(n: u64) -> [u8; 32] {
    let mut cmu = [0; 32];
    cmu[..8].copy_from_slice(&n.to_le_bytes());
    cmu
}

fn shielded(height: u32, sapling: &[u64], orchard: &[u64], ironwood: &[u64]) -> CompactTransaction {
    let action = |&n: &u64| CompactShieldedAction {
        nullifier: [0; 32],
        commitment: pallas::Base::from(n).to_repr(),
        ephemeral_key: [0; 32],
        ciphertext: [0; 52],
    };
    CompactTransaction {
        index: 1,
        txid: transaction::Hash(hash(height).0),
        sapling_spends: Vec::new(),
        sapling_outputs: sapling
            .iter()
            .map(|&n| CompactSaplingOutput {
                cmu: cmu(n),
                ephemeral_key: [0; 32],
                ciphertext: [0; 52],
            })
            .collect(),
        orchard_actions: orchard.iter().map(action).collect(),
        ironwood_actions: ironwood.iter().map(action).collect(),
    }
}

/// The blocks of the epoch under test, 1,000 to 2,999. Block 1,500 adds 4 Sapling and 2
/// Orchard commitments. Block 2,100 adds 5 Sapling commitments, the second of which completes
/// subtree 15, and 1 Ironwood commitment.
fn blocks() -> Vec<CompactBlockRecord> {
    (1_000..3_000)
        .map(|height| {
            let transactions = match height {
                1_500 => vec![shielded(height, &[1, 2, 3, 4], &[1, 2], &[])],
                2_100 => vec![shielded(height, &[5, 6, 7, 8, 9], &[], &[3])],
                _ => Vec::new(),
            };
            record(height, transactions)
        })
        .collect()
}

impl Trees {
    /// Appends the commitments of `block` one at a time, without `append_batch`. Skips a
    /// commitment that is not a field element.
    fn append_one_by_one(&mut self, block: &CompactBlockRecord, completed: &mut Vec<SubtreeRoot>) {
        let txs = &block.transactions;
        for output in txs.iter().flat_map(|tx| &tx.sapling_outputs) {
            let Some(leaf) = sapling::tree::NoteCommitmentUpdate::from_bytes(&output.cmu).into()
            else {
                continue;
            };
            self.sapling.append(leaf).unwrap();
            completed.extend(
                self.sapling
                    .completed_subtree_index_and_root()
                    .map(|(index, node)| SubtreeRoot { index, root: SubtreeNode::Sapling(node) }),
            );
        }
        for action in txs.iter().flat_map(|tx| &tx.orchard_actions) {
            self.orchard.append(pallas::Base::from_repr(action.commitment).unwrap()).unwrap();
            completed.extend(
                self.orchard
                    .completed_subtree_index_and_root()
                    .map(|(index, node)| SubtreeRoot { index, root: SubtreeNode::Orchard(node) }),
            );
        }
        for action in txs.iter().flat_map(|tx| &tx.ironwood_actions) {
            self.ironwood.append(pallas::Base::from_repr(action.commitment).unwrap()).unwrap();
            completed.extend(
                self.ironwood
                    .completed_subtree_index_and_root()
                    .map(|(index, node)| SubtreeRoot { index, root: SubtreeNode::Ironwood(node) }),
            );
        }
    }

    /// The frontiers of the trees, with an empty frontier for an empty tree.
    fn frontiers(&self) -> Frontiers {
        let encode = |count: u64, bytes: Vec<u8>| if count == 0 { Vec::new() } else { bytes };
        Frontiers {
            sapling: encode(self.sapling.count(), self.sapling.to_rpc_bytes()),
            orchard: encode(self.orchard.count(), self.orchard.to_rpc_bytes()),
            ironwood: encode(self.ironwood.count(), self.ironwood.to_rpc_bytes()),
        }
    }
}

struct Fixture {
    /// The entry before the epoch, then one entry per range of the epoch.
    entries: Vec<ManifestEntry>,
    blobs: Vec<Bytes>,
    start: Frontiers,
    end: Trees,
    completed: Vec<SubtreeRoot>,
}

/// The ranges of `blocks` after [`start_tree`] with `seed`. The tree sizes count the
/// commitments of each block. The frontiers and the subtree roots come from
/// [`Trees::append_one_by_one`].
fn fixture(seed: u8, mut blocks: Vec<CompactBlockRecord>) -> Fixture {
    let mut sizes = TreeSizes { sapling: START_SAPLING, ..TreeSizes::default() };
    let mut entries = synthetic_entries(&[sizes]);
    let mut trees = Trees {
        sapling: start_tree(seed),
        orchard: Default::default(),
        ironwood: Default::default(),
    };
    let (start, mut completed, mut blobs) = (trees.frontiers(), Vec::new(), Vec::new());
    for range in blocks.chunks_mut(RANGE_SIZE as usize) {
        let first_completed = completed.len();
        for block in range.iter_mut() {
            trees.append_one_by_one(block, &mut completed);
            sizes = sizes.checked_add(block.additions()).unwrap();
            block.end_tree_sizes = sizes;
        }
        let blob = encode_range(range).unwrap();
        let mut entry = ManifestEntry::from_records_and_blob(range, &blob);
        entry.frontiers = trees.frontiers();
        entry.subtree_roots = completed[first_completed..].to_vec();
        entries.push(entry);
        blobs.push(blob.into());
    }
    Fixture { entries, blobs, start, end: trees, completed }
}

/// Verifies the epoch with the blobs of `edit`ed blocks. The entries describe the forged
/// blobs, but keep the honest frontiers and subtree roots, as a node would attest them.
fn verify_forged(edit: impl FnOnce(&mut [CompactBlockRecord])) -> Result<EpochProof, Reject> {
    let honest = fixture(1, blocks());
    let mut blocks = blocks();
    edit(&mut blocks);
    let mut forged = fixture(1, blocks);
    for (forged, honest) in forged.entries.iter_mut().zip(&honest.entries) {
        forged.frontiers = honest.frontiers.clone();
        forged.subtree_roots = honest.subtree_roots.clone();
    }
    EpochProof::rebuild(&Epochs::new(&forged.entries).nth(1).unwrap(), &forged.blobs, &forged.start)
}

fn assert_rejected(result: Result<EpochProof, Reject>, check: Check, height: u32) {
    assert_eq!(result, Err(Reject::Rejected { check, height }));
}

#[test]
fn an_epoch_rebuilds_to_the_manifest_frontiers() {
    let fixture = fixture(1, blocks());
    let epochs: Vec<_> = Epochs::new(&fixture.entries).collect();
    assert_eq!(epochs.len(), 2);
    assert_eq!(epochs[1].previous(), Some(&fixture.entries[0]));

    let proof = EpochProof::rebuild(&epochs[1], &fixture.blobs, &fixture.start).unwrap();

    assert_eq!(proof.end_frontiers, fixture.entries[2].frontiers);
    assert_eq!(proof.end_tree_sizes, fixture.entries[2].end_tree_sizes);
    assert_eq!(
        proof.roots,
        PoolRoots {
            sapling: Some(fixture.end.sapling.root()),
            orchard: Some(fixture.end.orchard.root()),
            ironwood: Some(fixture.end.ironwood.root()),
        }
    );
    assert_eq!(proof.completed, fixture.completed);
    assert_eq!(proof.completed.len(), 1);
    assert_eq!(proof.completed[0].index, NoteCommitmentSubtreeIndex(15));
    assert_eq!(fixture.entries[2].subtree_roots, fixture.completed);
}

/// Each row forges the blocks of the second epoch. The epoch must refuse the forged blobs
/// at the expected check and height.
#[test]
fn forged_blocks_are_rejected() {
    type Forge = fn(&mut [CompactBlockRecord]);
    let cases: [(&str, Forge, Check, u32); 3] = [
        (
            "a mutated commitment",
            |blocks| blocks[500].transactions[0].sapling_outputs[0].cmu = cmu(99),
            Check::Frontier,
            1_999,
        ),
        (
            "a dropped output",
            |blocks| {
                blocks[500].transactions[0].sapling_outputs.pop();
            },
            Check::Frontier,
            1_999,
        ),
        (
            "a commitment that is not a field element",
            |blocks| blocks[500].transactions[0].sapling_outputs[0].cmu = [0xff; 32],
            Check::Commitment,
            1_500,
        ),
    ];

    for (case, forge, check, height) in cases {
        let result = verify_forged(forge);

        assert_eq!(result, Err(Reject::Rejected { check, height }), "{case}");
    }
}

#[test]
fn a_wrong_start_frontier_is_rejected() {
    let fixture = fixture(1, blocks());
    let epoch = Epochs::new(&fixture.entries).nth(1).unwrap();
    let verify = |sapling: Vec<u8>| {
        EpochProof::rebuild(&epoch, &fixture.blobs, &Frontiers { sapling, ..fixture.start.clone() })
    };

    // Other nodes at the same tree size: the first range end does not match.
    assert_rejected(verify(start_tree(2).to_rpc_bytes()), Check::Frontier, 1_999);
    // An empty tree, or bytes that do not parse.
    assert_rejected(verify(Vec::new()), Check::StartFrontier, 1_000);
    assert_rejected(verify(vec![0xff; 3]), Check::StartFrontier, 1_000);
}

#[test]
fn wrong_subtree_roots_are_rejected() {
    let mut fixture = fixture(1, blocks());
    fixture.entries[2].subtree_roots.clear();

    let result = EpochProof::rebuild(
        &Epochs::new(&fixture.entries).nth(1).unwrap(),
        &fixture.blobs,
        &fixture.start,
    );

    assert_rejected(result, Check::SubtreeRoots, 2_999);
}

#[test]
fn a_wrong_blob_count_is_refused() {
    let fixture = fixture(1, blocks());
    let epoch = Epochs::new(&fixture.entries).nth(1).unwrap();
    let mut blobs = fixture.blobs.clone();

    assert_eq!(
        EpochProof::rebuild(&epoch, &blobs[..1], &fixture.start),
        Err(Reject::BlobCount { entries: 2, blobs: 1 })
    );
    blobs.push(blobs[0].clone());
    assert_eq!(
        EpochProof::rebuild(&epoch, &blobs, &fixture.start),
        Err(Reject::BlobCount { entries: 2, blobs: 3 })
    );
}

#[test]
fn a_pool_that_becomes_inactive_is_rejected() {
    let fixture = fixture(1, blocks());
    let epoch = Epochs::new(&fixture.entries).nth(1).unwrap();
    // Ironwood is active at the start with an empty tree, and the first range end has no
    // Ironwood frontier. The trees agree, because both are empty.
    let start = Frontiers {
        ironwood: ironwood::tree::NoteCommitmentTree::default().to_rpc_bytes(),
        ..fixture.start.clone()
    };
    assert!(fixture.entries[1].frontiers.ironwood.is_empty());

    let result = EpochProof::rebuild(&epoch, &fixture.blobs, &start);

    assert_rejected(result, Check::Frontier, 1_999);
}
