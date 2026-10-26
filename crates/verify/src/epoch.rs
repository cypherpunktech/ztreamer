//! Verification epochs: groups of consecutive ranges that the node attests with one call.
//!
//! `z_gettreestate` takes 0.2 to 2.6 s per call. One call per range is too slow, so the replica
//! asks for the tree roots once per epoch, at the end of its last range. The epoch rule keeps
//! each epoch small enough to rebuild in one pass: an epoch closes after [`MAX_RANGES`] ranges,
//! or at the first range that brings the note commitments it adds to [`MAX_COMMITMENTS`]. The
//! last range of the manifest also closes an epoch. Mainnet has about 400 epochs.

use ztreamer_indexer::{codec::TreeSizes, index::range_meta::ManifestEntry};

/// The most ranges in one epoch.
pub const MAX_RANGES: usize = 10;

/// An epoch closes at the first range that brings the note commitments it adds, over all three
/// pools, to this count.
pub const MAX_COMMITMENTS: u64 = 1_000_000;

/// Consecutive manifest entries that are verified together.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Epoch<'a> {
    entries: &'a [ManifestEntry],
    commitments: u64,
}

impl<'a> Epoch<'a> {
    /// The entries of the epoch, in height order. Never empty.
    pub const fn entries(&self) -> &'a [ManifestEntry] {
        self.entries
    }

    /// The last height of the epoch. The node attests the tree roots at this height.
    pub const fn end_height(&self) -> u32 {
        self.entries.last().expect("an epoch is never empty").end_height
    }

    /// The note commitments that the epoch adds, over all three pools.
    pub const fn commitments(&self) -> u64 {
        self.commitments
    }
}

/// Groups `entries` into epochs with the rule in the module docs.
///
/// `entries` must be the ranges of a manifest in ascending order from genesis, with tree sizes
/// that do not decrease. [`Manifest::validate`](crate::manifest::Manifest::validate) checks
/// this. The function does not read `epoch_end`.
///
/// ```
/// use ztreamer_verify::{epoch::epochs, manifest::Manifest};
///
/// # fn run(manifest: Manifest) {
/// for epoch in epochs(&manifest.entries) {
///     println!("{} ranges through {}", epoch.entries().len(), epoch.end_height());
/// }
/// # }
/// ```
pub fn epochs(entries: &[ManifestEntry]) -> Vec<Epoch<'_>> {
    let mut epochs = Vec::new();
    let (mut start, mut base) = (0, 0);
    for (index, entry) in entries.iter().enumerate() {
        let commitments = total(entry.end_tree_sizes).saturating_sub(base);
        let closes = index + 1 - start == MAX_RANGES
            || commitments >= MAX_COMMITMENTS
            || index + 1 == entries.len();
        if closes {
            epochs.push(Epoch { entries: &entries[start..=index], commitments });
            (start, base) = (index + 1, total(entry.end_tree_sizes));
        }
    }
    epochs
}

/// The note commitments in all three trees.
fn total(sizes: TreeSizes) -> u64 {
    [sizes.sapling, sizes.orchard, sizes.ironwood].into_iter().map(u64::from).sum()
}

/// Contiguous entries from genesis. Entry `i` adds `adds[i]` commitments to the trees, and the
/// `epoch_end` flags agree with [`epochs`].
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
            sizes = TreeSizes {
                sapling: sizes.sapling + add.sapling,
                orchard: sizes.orchard + add.orchard,
                ironwood: sizes.ironwood + add.ironwood,
            };
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
    let mut end = 0;
    let closes: Vec<_> = epochs(&entries)
        .iter()
        .map(|epoch| {
            end += epoch.entries().len();
            end - 1
        })
        .collect();
    for index in closes {
        entries[index].epoch_end = true;
    }
    entries
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sapling(count: u32) -> TreeSizes {
        TreeSizes { sapling: count, ..TreeSizes::default() }
    }

    fn lengths(entries: &[ManifestEntry]) -> Vec<usize> {
        epochs(entries).iter().map(|epoch| epoch.entries().len()).collect()
    }

    #[test]
    fn an_epoch_closes_at_ten_ranges() {
        let entries = synthetic_entries(&[sapling(1_000); 25]);
        let epochs = epochs(&entries);

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
        assert_eq!(epochs(&entries)[0].commitments(), MAX_COMMITMENTS);

        // A heavy range closes the epoch that it joins.
        let entries = synthetic_entries(&[sapling(10), sapling(1_060_000), sapling(10)]);
        assert_eq!(lengths(&entries), [2, 1]);
    }

    #[test]
    fn commitments_count_all_three_pools() {
        let add = TreeSizes { sapling: 200_000, orchard: 200_000, ironwood: 100_000 };
        let entries = synthetic_entries(&[add; 4]);

        assert_eq!(lengths(&entries), [2, 2]);
        assert_eq!(epochs(&entries)[1].commitments(), 1_000_000);
    }

    #[test]
    fn no_entries_give_no_epochs() {
        assert!(epochs(&[]).is_empty());
    }
}
