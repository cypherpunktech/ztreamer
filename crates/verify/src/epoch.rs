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

use bytes::Bytes;
use pasta_curves::{group::ff::PrimeField, pallas};
use zakura_chain::{ironwood, orchard, sapling};
use ztreamer_indexer::{
    codec::{CompactBlockRecord, TreeSizes},
    index::CheckedRange,
    manifest_entry::{Frontiers, MAX_COMMITMENTS, MAX_RANGES, ManifestEntry},
    pool::{PoolRoots, SubtreeNode, SubtreeRoot},
    types::CompactShieldedAction,
};
use ztreamer_remote::frontier::{FromRpc, RpcDecodeError};

use crate::range::{Check, RangeBlob, RangeLink, Reject};

/// Consecutive manifest entries that are verified together.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Epoch<'a> {
    entries: &'a [ManifestEntry],
    previous: Option<&'a ManifestEntry>,
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

    /// Returns `true` if the epoch rule closes the epoch at its last range: the epoch has
    /// [`MAX_RANGES`] ranges, or it adds [`MAX_COMMITMENTS`] note commitments.
    ///
    /// Returns `false` for a shorter last epoch of a manifest. Only the end of the manifest
    /// closes that epoch, and a longer manifest continues it.
    pub fn closed_by_rule(&self) -> bool {
        let base = self.previous.map_or(0, |previous| previous.end_tree_sizes.total());
        let last = self.entries.last().expect("an epoch is never empty");
        self.entries.len() >= MAX_RANGES
            || last.end_tree_sizes.total().saturating_sub(base) >= MAX_COMMITMENTS
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
    /// empty. An active pool that becomes inactive is [`Check::Frontier`]. The proof has a root
    /// for each pool, also for an empty tree. [`EpochProof::matches_roots`] compares these roots
    /// with the node roots.
    pub fn verify(&self, blobs: &[Bytes], start: &Frontiers) -> Result<EpochProof, Reject> {
        self.verify_ranges(blobs, start).map(|(proof, _)| proof)
    }

    /// [`Epoch::verify`], and the [`CheckedRange`] of each blob, in order. The importer installs
    /// them with [`Index::install_checked_range`]. Thus the install does not check the blobs
    /// again.
    ///
    /// [`Index::install_checked_range`]: ztreamer_indexer::index::Index::install_checked_range
    pub(crate) fn verify_ranges(
        &self,
        blobs: &[Bytes],
        start: &Frontiers,
    ) -> Result<(EpochProof, Vec<CheckedRange>), Reject> {
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
        let mut ranges = Vec::with_capacity(blobs.len());
        for (entry, blob) in self.entries.iter().zip(blobs) {
            let first_completed = completed.len();
            let (end, range) = RangeBlob::new(blob, entry).check_with(&link, |record| {
                trees.append(record, &mut completed).map_err(|check| check.at(record.height))
            })?;
            link = end;
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
            ranges.push(range);
        }
        let proof = EpochProof {
            end_frontiers: end_frontiers.clone(),
            end_tree_sizes: link.end_tree_sizes,
            roots: trees.roots(),
            completed,
        };
        Ok((proof, ranges))
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

    /// Groups `entries` into epochs, from the entry at `start`. `start` must be the first entry
    /// of an epoch of [`Epochs::new`]. Then the epochs are the epochs of [`Epochs::new`] from
    /// that epoch.
    pub(crate) const fn starting_at(entries: &'a [ManifestEntry], start: usize) -> Self {
        Self { entries, start }
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
        Some(Epoch { entries, previous })
    }
}

impl FusedIterator for Epochs<'_> {}

/// What an epoch proves when its ranges check out and its trees rebuild.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EpochProof {
    /// The frontiers at the end of the epoch: the frontiers of its last entry, which the rebuild
    /// gave byte for byte. They are the start frontiers of the next epoch.
    pub end_frontiers: Frontiers,
    /// The tree sizes at [`Epoch::end_height`]. They are the sizes of the rebuilt trees.
    pub end_tree_sizes: TreeSizes,
    /// The rebuilt roots at [`Epoch::end_height`]. Each root is `Some`: a tree with no
    /// commitments has the root of the empty tree. To compare these roots with the
    /// `z_gettreestate` answer of the node, use [`EpochProof::matches_roots`].
    pub roots: PoolRoots,
    /// The subtrees that the epoch completed, in the order that its blocks completed them.
    pub completed: Vec<SubtreeRoot>,
}

impl EpochProof {
    /// Returns `true` if each root in `node_roots` matches the rebuilt tree of its pool.
    /// `node_roots` are the roots of the node at [`Epoch::end_height`].
    ///
    /// The method uses one of these rules for each pool:
    /// - The node gives a root: the root must be equal to the rebuilt root.
    /// - The node gives no root, because the pool is not active: the rebuilt tree must have 0
    ///   commitments.
    ///
    /// The method uses the tree size, not the frontier. The frontier cannot show if a pool with
    /// an empty tree is active: both cases can have the serialized empty frontier `000000`.
    pub fn matches_roots(&self, node_roots: &PoolRoots) -> bool {
        let (rebuilt, sizes) = (&self.roots, self.end_tree_sizes);
        node_roots.sapling.map_or(sizes.sapling == 0, |root| rebuilt.sapling == Some(root))
            && node_roots.orchard.map_or(sizes.orchard == 0, |root| rebuilt.orchard == Some(root))
            && node_roots
                .ironwood
                .map_or(sizes.ironwood == 0, |root| rebuilt.ironwood == Some(root))
    }
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

    /// The root of each tree. An empty tree has the root of the empty tree.
    pub(crate) fn roots(&self) -> PoolRoots {
        PoolRoots {
            sapling: Some(self.sapling.root()),
            orchard: Some(self.orchard.root()),
            ironwood: Some(self.ironwood.root()),
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
        index::RANGE_SIZE,
        manifest_entry::{BlobDigest, Frontiers},
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
mod tests;
