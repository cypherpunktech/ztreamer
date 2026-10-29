//! The proof of an epoch: what its ranges prove when they check out and its trees rebuild, and
//! the attestation of that proof by a node.
//!
//! [`EpochProof::rebuild`] checks each range of an [`Epoch`] and rebuilds the three note
//! commitment trees from the start frontier of the epoch. Each frontier in the manifest inside
//! the epoch is a checkpoint. [`EpochProof::attest`] then asks the node about the proof once:
//! the block hash at the end of each range, the three tree roots at the end of the epoch, and
//! the roots of the subtrees that the epoch completed, with one call per pool.

use bytes::Bytes;
use pasta_curves::{group::ff::PrimeField, pallas};
use zakura_chain::{ironwood, orchard, sapling, subtree::NoteCommitmentSubtreeIndex};
use ztreamer_chain::{
    codec::{CompactBlockRecord, TreeSizes},
    types::CompactShieldedAction,
};
use ztreamer_indexer::{
    index::{CheckedRange, range_meta::Attested},
    manifest_entry::Frontiers,
    pool::{Pool, PoolRoots, SubtreeNode, SubtreeRoot},
};
use ztreamer_remote::frontier::{FromRpc, RpcDecodeError};

use crate::{
    attest::{Attestor, EpochError},
    epoch::Epoch,
    range::{Check, RangeBlob, RangeLink, Reject},
};

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
    /// Checks each range of `epoch` and rebuilds the note commitment trees from `start`.
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
    pub fn rebuild(epoch: &Epoch<'_>, blobs: &[Bytes], start: &Frontiers) -> Result<Self, Reject> {
        Self::rebuild_ranges(epoch, blobs, start).map(|(proof, _)| proof)
    }

    /// [`EpochProof::rebuild`], and the [`CheckedRange`] of each blob, in order. The importer installs
    /// them with [`Index::install_checked_range`]. Thus the install does not check the blobs
    /// again.
    ///
    /// [`Index::install_checked_range`]: ztreamer_indexer::index::Index::install_checked_range
    pub(crate) fn rebuild_ranges(
        epoch: &Epoch<'_>,
        blobs: &[Bytes],
        start: &Frontiers,
    ) -> Result<(Self, Vec<CheckedRange>), Reject> {
        if blobs.len() != epoch.entries().len() {
            return Err(Reject::BlobCount { entries: epoch.entries().len(), blobs: blobs.len() });
        }
        let mut link = epoch.previous().map_or(RangeLink::GENESIS, RangeLink::from);
        let start_height = epoch.entries().first().map_or(0, |entry| entry.start_height);
        let mut trees = Trees::try_from(start)
            .ok()
            .filter(|trees| trees.have_sizes(link.end_tree_sizes))
            .ok_or(Check::StartFrontier.at(start_height))?;

        let (mut end_frontiers, mut completed) = (start, Vec::new());
        let mut ranges = Vec::with_capacity(blobs.len());
        for (entry, blob) in epoch.entries().iter().zip(blobs) {
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
        let proof = Self {
            end_frontiers: end_frontiers.clone(),
            end_tree_sizes: link.end_tree_sizes,
            roots: trees.roots(),
            completed,
        };
        Ok((proof, ranges))
    }

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

    /// Compares the proof of `epoch`, the result of [`EpochProof::rebuild`], with the answers of
    /// `attestor`, and returns one [`Attested`] record per entry, in order.
    ///
    /// The method compares the end hash of each entry with the node. It compares the three
    /// roots of the proof at [`Epoch::end_height`] with [`EpochProof::matches_roots`]. It compares
    /// the root of each subtree that the epoch completed, with one [`Attestor::subtree_roots`]
    /// call per pool. A wrong subtree root rejects at the end height of the entry that lists it.
    ///
    /// The record of an entry holds the end hash of the node, the subtrees that the entry
    /// completed, and the roots at its end. The last entry has the roots of the node. An entry
    /// before it has the rebuilt roots of its frontiers: the node attests them through the end
    /// roots, because the rebuild appends the leaves of the epoch to these trees. If the node
    /// gives no root for a pool at the end, the pool is not active before the end. Then the root
    /// of that pool is `None` in each record. In the epoch where a pool becomes active, an entry
    /// before the activation height has the root of the empty tree, not `None`. The frontier of
    /// the entry cannot show the difference.
    ///
    /// ```
    /// use ztreamer_indexer::{index::range_meta::Attested, manifest_entry::Frontiers};
    /// use ztreamer_verify::{
    ///     attest::{Attestor, EpochError},
    ///     epoch::Epoch,
    ///     proof::EpochProof,
    /// };
    ///
    /// # async fn run(
    /// #     epoch: Epoch<'_>,
    /// #     blobs: Vec<bytes::Bytes>,
    /// #     start: Frontiers,
    /// #     node: &dyn Attestor,
    /// # ) -> Result<Vec<Attested>, EpochError> {
    /// let proof = EpochProof::rebuild(&epoch, &blobs, &start)?;
    /// proof.attest(&epoch, node).await
    /// # }
    /// ```
    pub async fn attest(
        &self,
        epoch: &Epoch<'_>,
        attestor: &dyn Attestor,
    ) -> Result<Vec<Attested>, EpochError> {
        for entry in epoch.entries() {
            let height = entry.end_height;
            let hash = attestor
                .block_hash(height)
                .await
                .map_err(|source| EpochError::Node { height, source })?;
            if hash != entry.end_hash {
                return Err(Check::AttestedHash.at(height).into());
            }
        }
        let end = epoch.end_height();
        let roots = attestor
            .tree_roots(end)
            .await
            .map_err(|source| EpochError::Node { height: end, source })?;
        if !self.matches_roots(&roots) {
            return Err(Check::AttestedRoots.at(end).into());
        }
        self.attest_subtrees(epoch, attestor).await?;
        epoch
            .entries()
            .iter()
            .map(|entry| {
                let roots = if entry.end_height == end {
                    roots
                } else {
                    // `EpochProof::rebuild` checked that the frontiers are the rebuilt trees, thus
                    // they parse.
                    let rebuilt = Trees::try_from(&entry.frontiers)
                        .map_err(|_| Check::Frontier.at(entry.end_height))?
                        .roots();
                    PoolRoots {
                        sapling: roots.sapling.and(rebuilt.sapling),
                        orchard: roots.orchard.and(rebuilt.orchard),
                        ironwood: roots.ironwood.and(rebuilt.ironwood),
                    }
                };
                let subtree_roots = entry.subtree_roots.clone();
                Ok(Attested { end_hash: entry.end_hash, roots, subtree_roots })
            })
            .collect()
    }

    /// Compares each subtree root of the proof with the node, with one call per pool.
    async fn attest_subtrees(
        &self,
        epoch: &Epoch<'_>,
        attestor: &dyn Attestor,
    ) -> Result<(), EpochError> {
        let end = epoch.end_height();
        for pool in Pool::ALL {
            let completed: Vec<_> =
                self.completed.iter().filter(|root| root.root.pool() == pool).collect();
            let indices = completed.iter().map(|root| root.index.0);
            let (Some(start), Some(last)) = (indices.clone().min(), indices.max()) else {
                continue;
            };
            let answers = attestor
                .subtree_roots(
                    pool,
                    NoteCommitmentSubtreeIndex(start),
                    (last - start).saturating_add(1),
                )
                .await
                .map_err(|source| EpochError::Node { height: end, source })?;
            let wrong = completed.into_iter().find(|completed| {
                answers.get(usize::from(completed.index.0 - start)) != Some(&completed.root)
            });
            if let Some(wrong) = wrong {
                let entry =
                    epoch.entries().iter().find(|entry| entry.subtree_roots.contains(wrong));
                let height = entry.map_or(end, |entry| entry.end_height);
                return Err(Check::AttestedSubtree.at(height).into());
            }
        }
        Ok(())
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

#[cfg(test)]
mod tests;
