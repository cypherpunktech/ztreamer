//! Verification epochs: groups of consecutive ranges that the node attests with one call.
//!
//! `z_gettreestate` takes 0.2 to 2.6 s per call. One call per range is too slow, so the replica
//! asks for the tree roots once per epoch, at the end of its last range. The epoch rule keeps
//! each epoch small enough to rebuild in one pass: an epoch closes after [`MAX_RANGES`] ranges,
//! or at the first range that brings the note commitments it adds to [`MAX_COMMITMENTS`]. The
//! last range of the manifest also closes an epoch. Mainnet has about 400 epochs.
//!
//! [`Epoch::verify`] checks each range of an epoch and rebuilds the three note commitment trees
//! from the start frontier of the epoch. Each frontier in the manifest inside the epoch is a
//! checkpoint. The node then attests the [`EpochProof`] roots once.

use std::iter::FusedIterator;

use pasta_curves::{group::ff::PrimeField, pallas};
use zakura_chain::{ironwood, orchard, sapling};
use ztreamer_indexer::{
    codec::{CompactBlockRecord, TreeSizes},
    frontier::{FromRpc, RpcDecodeError},
    index::range_meta::{Frontiers, ManifestEntry, PoolRoots, SubtreeNode, SubtreeRoot},
    types::CompactShieldedAction,
};

use crate::range::{Check, RangeBlob, RangeLink, Reject};

/// The most ranges in one epoch.
pub const MAX_RANGES: usize = 10;

/// An epoch closes at the first range that brings the note commitments it adds, over all three
/// pools, to this count.
pub const MAX_COMMITMENTS: u64 = 1_000_000;

/// Consecutive manifest entries that are verified together.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Epoch<'a> {
    entries: &'a [ManifestEntry],
    previous: Option<&'a ManifestEntry>,
    commitments: u64,
}

impl<'a> Epoch<'a> {
    /// The entries of the epoch, in height order. Never empty.
    pub const fn entries(&self) -> &'a [ManifestEntry] {
        self.entries
    }

    /// The entry before the epoch, or `None` for the epoch at genesis. The first range of the
    /// epoch links to its end hash and end tree sizes.
    pub const fn previous(&self) -> Option<&'a ManifestEntry> {
        self.previous
    }

    /// The last height of the epoch. The node attests the tree roots at this height.
    pub const fn end_height(&self) -> u32 {
        self.entries.last().expect("an epoch is never empty").end_height
    }

    /// The note commitments that the epoch adds, over all three pools.
    pub const fn commitments(&self) -> u64 {
        self.commitments
    }

    /// Checks each range of the epoch and rebuilds the note commitment trees from `start`.
    ///
    /// `blobs` holds one blob per entry, in order; another count is [`Reject::BlobCount`].
    /// `start` holds the frontiers after [`Epoch::previous`], as the previous epoch attested
    /// them. For each range, the method runs [`RangeBlob::check`], appends the commitments of
    /// each block (Sapling `cmu`s, Orchard and Ironwood `cmx`s), and then compares the trees
    /// with the entry: the frontiers byte for byte, and the completed subtrees with
    /// `subtree_roots`.
    ///
    /// An empty frontier means that the pool is not active, as in the manifest: its tree is
    /// empty, and its root in the proof is `None`. An active pool that becomes inactive is
    /// [`Check::Frontier`].
    pub fn verify(
        &self,
        blobs: &[impl AsRef<[u8]>],
        start: &Frontiers,
    ) -> Result<EpochProof, Reject> {
        if blobs.len() != self.entries.len() {
            return Err(Reject::BlobCount { entries: self.entries.len(), blobs: blobs.len() });
        }
        let mut link = self.previous.map_or(RangeLink::GENESIS, RangeLink::from);
        let start_height = self.entries.first().map_or(0, |entry| entry.start_height);
        let mut trees = Trees::try_from(start)
            .ok()
            .filter(|trees| trees.have_sizes(link.end_tree_sizes))
            .ok_or(Check::StartFrontier.at(start_height))?;

        let (mut end_frontiers, mut completed) = (start, Vec::new());
        for (entry, blob) in self.entries.iter().zip(blobs) {
            let first_completed = completed.len();
            link = RangeBlob::new(blob.as_ref(), entry).check_with(&link, |record| {
                trees.append(record, &mut completed).map_err(|check| check.at(record.height))
            })?;
            if !entry.frontiers.follows(end_frontiers) || !trees.matches(&entry.frontiers) {
                return Err(Check::Frontier.at(entry.end_height));
            }
            let range_completed = &completed[first_completed..];
            if range_completed.len() != entry.subtree_roots.len()
                || range_completed.iter().any(|root| !entry.subtree_roots.contains(root))
            {
                return Err(Check::SubtreeRoots.at(entry.end_height));
            }
            end_frontiers = &entry.frontiers;
        }
        Ok(EpochProof {
            end_frontiers: end_frontiers.clone(),
            roots: trees.roots(end_frontiers),
            completed,
        })
    }
}

/// An iterator over the epochs of manifest entries, in height order.
///
/// Each item is the next [`Epoch`] with the rule in the module docs. The entries must be the
/// ranges of a manifest in ascending order from genesis, with tree sizes that do not decrease;
/// a [`Manifest`](crate::manifest::Manifest) always is. The iterator does not read `epoch_end`.
///
/// ```
/// use ztreamer_verify::manifest::Manifest;
///
/// # fn run(manifest: Manifest) {
/// for epoch in manifest.epochs() {
///     println!("{} ranges through {}", epoch.entries().len(), epoch.end_height());
/// }
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct Epochs<'a> {
    entries: &'a [ManifestEntry],
    start: usize,
}

impl<'a> Epochs<'a> {
    /// Groups `entries` into epochs, from the first entry.
    pub const fn new(entries: &'a [ManifestEntry]) -> Self {
        Self { entries, start: 0 }
    }
}

impl<'a> Iterator for Epochs<'a> {
    type Item = Epoch<'a>;

    fn next(&mut self) -> Option<Epoch<'a>> {
        let rest = self.entries.get(self.start..).filter(|rest| !rest.is_empty())?;
        let previous = self.start.checked_sub(1).map(|previous| &self.entries[previous]);
        let base = previous.map_or(0, |previous| previous.end_tree_sizes.total());
        let added = |entry: &ManifestEntry| entry.end_tree_sizes.total().saturating_sub(base);
        let len = rest
            .iter()
            .take(MAX_RANGES)
            .position(|entry| added(entry) >= MAX_COMMITMENTS)
            .map_or(rest.len().min(MAX_RANGES), |at| at + 1);
        let entries = &rest[..len];
        self.start += len;
        Some(Epoch { entries, previous, commitments: added(&entries[len - 1]) })
    }
}

impl FusedIterator for Epochs<'_> {}

/// What an epoch proves when its ranges check out and its trees rebuild.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EpochProof {
    /// The frontiers at the end of the epoch: the frontiers of its last entry, which the rebuild
    /// gave byte for byte. They are the start frontiers of the next epoch.
    pub end_frontiers: Frontiers,
    /// The rebuilt roots at [`Epoch::end_height`]. Compare them with the `z_gettreestate` answer
    /// of the node. A root is `None` when the end frontier of its pool is empty: the pool is not
    /// active, and the node gives no root for it.
    pub roots: PoolRoots,
    /// The subtrees that the epoch completed, in the order that its blocks completed them.
    pub completed: Vec<SubtreeRoot>,
}

/// The three note commitment trees during a rebuild.
pub(crate) struct Trees {
    sapling: sapling::tree::NoteCommitmentTree,
    orchard: orchard::tree::NoteCommitmentTree,
    ironwood: ironwood::tree::NoteCommitmentTree,
}

impl TryFrom<&Frontiers> for Trees {
    type Error = RpcDecodeError;

    /// Reads the trees from `frontiers`. An empty frontier, a pool that is not active, is an
    /// empty tree.
    fn try_from(frontiers: &Frontiers) -> Result<Self, RpcDecodeError> {
        fn parse<T: FromRpc + Default>(bytes: &[u8]) -> Result<T, RpcDecodeError> {
            if bytes.is_empty() { Ok(T::default()) } else { T::from_rpc_bytes(bytes) }
        }
        Ok(Self {
            sapling: parse(&frontiers.sapling)?,
            orchard: parse(&frontiers.orchard)?,
            ironwood: parse(&frontiers.ironwood)?,
        })
    }
}

impl Trees {
    fn have_sizes(&self, sizes: TreeSizes) -> bool {
        self.sapling.count() == u64::from(sizes.sapling)
            && self.orchard.count() == u64::from(sizes.orchard)
            && self.ironwood.count() == u64::from(sizes.ironwood)
    }

    /// Appends the commitments of one block, and pushes the subtrees that they complete.
    fn append(
        &mut self,
        record: &CompactBlockRecord,
        completed: &mut Vec<SubtreeRoot>,
    ) -> Result<(), Check> {
        let leaves = Leaves::try_from(record)?;
        let subtree = |index, root| SubtreeRoot { index, root };
        let sapling = self.sapling.append_batch(&leaves.sapling).map_err(|_| Check::Commitment)?;
        let orchard = self.orchard.append_batch(&leaves.orchard).map_err(|_| Check::Commitment)?;
        let ironwood =
            self.ironwood.append_batch(&leaves.ironwood).map_err(|_| Check::Commitment)?;
        completed.extend(
            [
                sapling.map(|(index, node)| subtree(index, SubtreeNode::Sapling(node))),
                orchard.map(|(index, node)| subtree(index, SubtreeNode::Orchard(node))),
                ironwood.map(|(index, node)| subtree(index, SubtreeNode::Ironwood(node))),
            ]
            .into_iter()
            .flatten(),
        );
        Ok(())
    }

    /// Compares the trees with `frontiers`. An empty frontier, a pool that is not active, must
    /// be an empty tree. Any other frontier must equal the RPC encoding of the tree byte for
    /// byte.
    fn matches(&self, frontiers: &Frontiers) -> bool {
        let same = |frontier: &[u8], count: u64, encoded: Vec<u8>| {
            if frontier.is_empty() { count == 0 } else { encoded == frontier }
        };
        same(&frontiers.sapling, self.sapling.count(), self.sapling.to_rpc_bytes())
            && same(&frontiers.orchard, self.orchard.count(), self.orchard.to_rpc_bytes())
            && same(&frontiers.ironwood, self.ironwood.count(), self.ironwood.to_rpc_bytes())
    }

    /// The roots of the trees. A root is `None` when the frontier of its pool is empty, because
    /// the node gives no root for a pool that is not active.
    pub(crate) fn roots(&self, frontiers: &Frontiers) -> PoolRoots {
        PoolRoots {
            sapling: (!frontiers.sapling.is_empty()).then(|| self.sapling.root()),
            orchard: (!frontiers.orchard.is_empty()).then(|| self.orchard.root()),
            ironwood: (!frontiers.ironwood.is_empty()).then(|| self.ironwood.root()),
        }
    }
}

/// The tree leaves of one block, per pool: Sapling `cmu`s, Orchard and Ironwood `cmx`s.
struct Leaves {
    sapling: Vec<sapling::tree::NoteCommitmentUpdate>,
    orchard: Vec<pallas::Base>,
    ironwood: Vec<pallas::Base>,
}

impl TryFrom<&CompactBlockRecord> for Leaves {
    type Error = Check;

    /// Reads the commitments of each pool. A commitment that is not a field element of its
    /// pool is [`Check::Commitment`].
    fn try_from(record: &CompactBlockRecord) -> Result<Self, Check> {
        let transactions = &record.transactions;
        let cmx = |action: &CompactShieldedAction| {
            Option::from(pallas::Base::from_repr(action.commitment))
        };
        Ok(Self {
            sapling: transactions
                .iter()
                .flat_map(|tx| &tx.sapling_outputs)
                .map(|output| {
                    Option::from(sapling::tree::NoteCommitmentUpdate::from_bytes(&output.cmu))
                })
                .collect::<Option<_>>()
                .ok_or(Check::Commitment)?,
            orchard: transactions
                .iter()
                .flat_map(|tx| &tx.orchard_actions)
                .map(cmx)
                .collect::<Option<_>>()
                .ok_or(Check::Commitment)?,
            ironwood: transactions
                .iter()
                .flat_map(|tx| &tx.ironwood_actions)
                .map(cmx)
                .collect::<Option<_>>()
                .ok_or(Check::Commitment)?,
        })
    }
}

/// Contiguous entries from genesis. Entry `i` adds `adds[i]` commitments to the trees, and the
/// `epoch_end` flags agree with [`Epochs`].
#[cfg(test)]
pub(crate) fn synthetic_entries(adds: &[TreeSizes]) -> Vec<ManifestEntry> {
    use ztreamer_indexer::{
        index::{
            RANGE_SIZE,
            range_meta::{BlobDigest, Frontiers},
        },
        test_utils::hash,
    };

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

#[cfg(test)]
mod tests {
    use incrementalmerkletree::{Position, frontier::Frontier};
    use zakura_chain::{subtree::NoteCommitmentSubtreeIndex, transaction};
    use ztreamer_indexer::{
        codec::encode_range,
        index::RANGE_SIZE,
        test_utils::{hash, record},
        types::{CompactSaplingOutput, CompactTransaction},
    };

    use super::*;
    use crate::range::entry_for;

    fn sapling(count: u32) -> TreeSizes {
        TreeSizes { sapling: count, ..TreeSizes::default() }
    }

    fn lengths(entries: &[ManifestEntry]) -> Vec<usize> {
        Epochs::new(entries).map(|epoch| epoch.entries().len()).collect()
    }

    #[test]
    fn an_epoch_closes_at_ten_ranges() {
        let entries = synthetic_entries(&[sapling(1_000); 25]);
        let epochs: Vec<_> = Epochs::new(&entries).collect();

        assert_eq!(lengths(&entries), [10, 10, 5]);
        assert_eq!(epochs[0].end_height(), 9_999);
        assert_eq!(epochs[0].commitments(), 10_000);
        assert_eq!(epochs[2].entries()[0].start_height, 20_000);
        assert_eq!(epochs[2].end_height(), 24_999);
    }

    #[test]
    fn an_epoch_closes_when_commitments_reach_the_cap() {
        // 400,000 per range: the third range brings the epoch to 1,200,000.
        assert_eq!(lengths(&synthetic_entries(&[sapling(400_000); 7])), [3, 3, 1]);

        // The cap is reached exactly at the second range.
        let entries = synthetic_entries(&[sapling(999_999), sapling(1), sapling(5)]);
        assert_eq!(lengths(&entries), [2, 1]);
        assert_eq!(Epochs::new(&entries).next().unwrap().commitments(), MAX_COMMITMENTS);

        // A heavy range closes the epoch that it joins.
        let entries = synthetic_entries(&[sapling(10), sapling(1_060_000), sapling(10)]);
        assert_eq!(lengths(&entries), [2, 1]);
    }

    #[test]
    fn commitments_count_all_three_pools() {
        let add = TreeSizes { sapling: 200_000, orchard: 200_000, ironwood: 100_000 };
        let entries = synthetic_entries(&[add; 4]);

        assert_eq!(lengths(&entries), [2, 2]);
        assert_eq!(Epochs::new(&entries).nth(1).unwrap().commitments(), 1_000_000);
    }

    #[test]
    fn no_entries_give_no_epochs() {
        let mut epochs = Epochs::new(&[]);

        assert_eq!(epochs.next(), None);
        assert_eq!(epochs.next(), None);
    }

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

    fn shielded(
        height: u32,
        sapling: &[u64],
        orchard: &[u64],
        ironwood: &[u64],
    ) -> CompactTransaction {
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
        fn append_one_by_one(
            &mut self,
            block: &CompactBlockRecord,
            completed: &mut Vec<SubtreeRoot>,
        ) {
            let txs = &block.transactions;
            for output in txs.iter().flat_map(|tx| &tx.sapling_outputs) {
                let Some(leaf) =
                    sapling::tree::NoteCommitmentUpdate::from_bytes(&output.cmu).into()
                else {
                    continue;
                };
                self.sapling.append(leaf).unwrap();
                completed.extend(
                    self.sapling.completed_subtree_index_and_root().map(|(index, node)| {
                        SubtreeRoot { index, root: SubtreeNode::Sapling(node) }
                    }),
                );
            }
            for action in txs.iter().flat_map(|tx| &tx.orchard_actions) {
                self.orchard.append(pallas::Base::from_repr(action.commitment).unwrap()).unwrap();
                completed.extend(
                    self.orchard.completed_subtree_index_and_root().map(|(index, node)| {
                        SubtreeRoot { index, root: SubtreeNode::Orchard(node) }
                    }),
                );
            }
            for action in txs.iter().flat_map(|tx| &tx.ironwood_actions) {
                self.ironwood.append(pallas::Base::from_repr(action.commitment).unwrap()).unwrap();
                completed.extend(
                    self.ironwood.completed_subtree_index_and_root().map(|(index, node)| {
                        SubtreeRoot { index, root: SubtreeNode::Ironwood(node) }
                    }),
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
        blobs: Vec<Vec<u8>>,
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
            let mut entry = entry_for(range, &blob);
            entry.frontiers = trees.frontiers();
            entry.subtree_roots = completed[first_completed..].to_vec();
            entries.push(entry);
            blobs.push(blob);
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
        Epochs::new(&forged.entries).nth(1).unwrap().verify(&forged.blobs, &forged.start)
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

        let proof = epochs[1].verify(&fixture.blobs, &fixture.start).unwrap();

        assert_eq!(proof.end_frontiers, fixture.entries[2].frontiers);
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

    #[test]
    fn a_mutated_commitment_is_rejected() {
        let result = verify_forged(|blocks| {
            blocks[500].transactions[0].sapling_outputs[0].cmu = cmu(99);
        });

        assert_rejected(result, Check::Frontier, 1_999);
    }

    #[test]
    fn a_dropped_output_is_rejected() {
        let result = verify_forged(|blocks| {
            blocks[500].transactions[0].sapling_outputs.pop();
        });

        assert_rejected(result, Check::Frontier, 1_999);
    }

    #[test]
    fn a_commitment_that_is_not_a_field_element_is_rejected() {
        let result = verify_forged(|blocks| {
            blocks[500].transactions[0].sapling_outputs[0].cmu = [0xff; 32];
        });

        assert_rejected(result, Check::Commitment, 1_500);
    }

    #[test]
    fn a_wrong_start_frontier_is_rejected() {
        let fixture = fixture(1, blocks());
        let epoch = Epochs::new(&fixture.entries).nth(1).unwrap();
        let verify = |sapling: Vec<u8>| {
            epoch.verify(&fixture.blobs, &Frontiers { sapling, ..fixture.start.clone() })
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

        let result =
            Epochs::new(&fixture.entries).nth(1).unwrap().verify(&fixture.blobs, &fixture.start);

        assert_rejected(result, Check::SubtreeRoots, 2_999);
    }

    #[test]
    fn a_wrong_blob_count_is_refused() {
        let fixture = fixture(1, blocks());
        let epoch = Epochs::new(&fixture.entries).nth(1).unwrap();
        let mut blobs = fixture.blobs.clone();

        assert_eq!(
            epoch.verify(&blobs[..1], &fixture.start),
            Err(Reject::BlobCount { entries: 2, blobs: 1 })
        );
        blobs.push(blobs[0].clone());
        assert_eq!(
            epoch.verify(&blobs, &fixture.start),
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

        let result = epoch.verify(&fixture.blobs, &start);

        assert_rejected(result, Check::Frontier, 1_999);
    }
}
