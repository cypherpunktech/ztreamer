//! Verification epochs: groups of consecutive ranges that the node attests with one call.
//!
//! `z_gettreestate` takes 0.2 to 2.6 s per call. One call per range is too slow, so the replica
//! asks for the tree roots once per epoch, at the end of its last range. The epoch rule keeps
//! each epoch small enough to rebuild in one pass: an epoch closes after [`MAX_RANGES`] ranges,
//! or at the first range that brings the note commitments it adds to [`MAX_COMMITMENTS`]. The
//! last range of the manifest also closes an epoch. Mainnet has about 400 epochs.
//!
//! An [`Epoch`] only groups manifest entries. `ztreamer_import::proof::EpochProof` checks
//! and rebuilds the ranges of an epoch, and the node attests the proof.

use std::iter::FusedIterator;

use crate::manifest_entry::{MAX_COMMITMENTS, MAX_RANGES, ManifestEntry};

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
}

/// An iterator over the epochs of manifest entries, in height order.
///
/// Each item is the next [`Epoch`] with the rule in the module docs. The entries must be the
/// ranges of a manifest in ascending order from genesis, with tree sizes that do not decrease;
/// a [`Manifest`](crate::manifest::Manifest) always is. The iterator does not read `epoch_end`.
///
/// ```
/// use ztreamer_snapshot::manifest::Manifest;
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
    pub const fn starting_at(entries: &'a [ManifestEntry], start: usize) -> Self {
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

#[cfg(test)]
mod tests {
    use ztreamer_chain::codec::TreeSizes;

    use super::*;
    use crate::test_utils::synthetic_entries;

    fn sapling(count: u32) -> TreeSizes {
        TreeSizes { sapling: count, ..TreeSizes::default() }
    }

    /// The note commitments that `epoch` adds, over all three pools.
    fn commitments(epoch: Epoch<'_>) -> u64 {
        let total = |entry: &ManifestEntry| entry.end_tree_sizes.total();
        total(epoch.entries().last().unwrap()) - epoch.previous().map_or(0, total)
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
        assert_eq!(commitments(epochs[0]), 10_000);
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
        assert_eq!(commitments(Epochs::new(&entries).next().unwrap()), MAX_COMMITMENTS);

        // A heavy range closes the epoch that it joins.
        let entries = synthetic_entries(&[sapling(10), sapling(1_060_000), sapling(10)]);
        assert_eq!(lengths(&entries), [2, 1]);
    }

    #[test]
    fn only_a_short_last_epoch_is_not_closed_by_the_rule() {
        let closed = |entries: &[ManifestEntry]| -> Vec<bool> {
            Epochs::new(entries).map(|epoch| epoch.closed_by_rule()).collect()
        };

        // Ten ranges close by count. The last 5 ranges close only at the end of the entries.
        assert_eq!(closed(&synthetic_entries(&[sapling(1_000); 25])), [true, true, false]);
        assert_eq!(closed(&synthetic_entries(&[sapling(1_000); 20])), [true, true]);
        // 1,200,000 commitments close by the cap. The last range adds 400,000 only.
        assert_eq!(closed(&synthetic_entries(&[sapling(400_000); 7])), [true, true, false]);
        // A last epoch that reaches the cap closes by the rule.
        assert_eq!(closed(&synthetic_entries(&[sapling(10), sapling(1_060_000)])), [true]);
    }

    #[test]
    fn commitments_count_all_three_pools() {
        let add = TreeSizes { sapling: 200_000, orchard: 200_000, ironwood: 100_000 };
        let entries = synthetic_entries(&[add; 4]);

        assert_eq!(lengths(&entries), [2, 2]);
        assert_eq!(commitments(Epochs::new(&entries).nth(1).unwrap()), 1_000_000);
    }

    #[test]
    fn no_entries_give_no_epochs() {
        let mut epochs = Epochs::new(&[]);

        assert_eq!(epochs.next(), None);
        assert_eq!(epochs.next(), None);
    }
}
