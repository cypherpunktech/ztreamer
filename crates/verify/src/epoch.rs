//! Verification epochs: groups of consecutive ranges that the node attests with one call.
//!
//! `z_gettreestate` takes 0.2 to 2.6 s per call. One call per range is too slow, so the replica
//! asks for the tree roots once per epoch, at the end of its last range. The epoch rule keeps
//! each epoch small enough to rebuild in one pass: an epoch closes after [`MAX_RANGES`] ranges,
//! or at the first range that brings the note commitments it adds to [`MAX_COMMITMENTS`]. The
//! last range of the manifest also closes an epoch. Mainnet has about 400 epochs.

use std::iter::FusedIterator;

use ztreamer_indexer::index::range_meta::ManifestEntry;

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
        Some(Epoch { entries, commitments: added(&entries[len - 1]) })
    }
}

impl FusedIterator for Epochs<'_> {}

/// Contiguous entries from genesis. Entry `i` adds `adds[i]` commitments to the trees, and the
/// `epoch_end` flags agree with [`Epochs`].
#[cfg(test)]
pub(crate) fn synthetic_entries(adds: &[ztreamer_indexer::codec::TreeSizes]) -> Vec<ManifestEntry> {
    use ztreamer_indexer::{
        codec::TreeSizes,
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
    use ztreamer_indexer::codec::TreeSizes;

    use super::*;

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
}
