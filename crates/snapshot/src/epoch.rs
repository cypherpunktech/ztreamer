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
///
/// [`Epoch::part_after`] gives the last entries of an epoch, as an epoch of their own. An
/// import uses it when the index is sealed inside an epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Epoch<'a> {
    entries: &'a [ManifestEntry],
    previous: Option<&'a ManifestEntry>,
    /// [`Epoch::closed_by_rule`] of the full epoch.
    closed_by_rule: bool,
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
    ///
    /// A part from [`Epoch::part_after`] gives the value of the full epoch.
    pub const fn closed_by_rule(&self) -> bool {
        self.closed_by_rule
    }

    /// The entries of the epoch after `height`, as an epoch. Its previous entry is the entry
    /// that ends at `height`, and it keeps [`Epoch::closed_by_rule`] of the full epoch.
    ///
    /// Returns the full epoch if `height` is the end of [`Epoch::previous`]. Returns `None` if
    /// `height` is not the end of an entry before the last entry of the epoch: then no entry of
    /// the epoch is after `height`, or `height` is not at a range end.
    ///
    /// ```
    /// use ztreamer_snapshot::manifest::Manifest;
    ///
    /// # fn run(manifest: Manifest) {
    /// // The index is sealed through 2,999, inside the first epoch.
    /// if let Some(part) = manifest.epochs().next().and_then(|epoch| epoch.part_after(2_999)) {
    ///     assert_eq!(part.entries()[0].start_height, 3_000);
    /// }
    /// # }
    /// ```
    pub fn part_after(self, height: u32) -> Option<Self> {
        if self.previous.map(|previous| previous.end_height) == Some(height) {
            return Some(self);
        }
        let at = self.entries.iter().position(|entry| entry.end_height == height)?;
        let (before, after) = self.entries.split_at(at + 1);
        (!after.is_empty()).then(|| Self { entries: after, previous: before.last(), ..self })
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
        let last = entries.last().expect("`rest` is not empty, thus `len` is at least 1");
        let closed_by_rule = len >= MAX_RANGES || added(last) >= MAX_COMMITMENTS;
        Some(Epoch { entries, previous, closed_by_rule })
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
    fn a_part_after_a_range_end_keeps_the_rule_of_the_full_epoch() {
        let entries = synthetic_entries(&[sapling(1_000); 25]);
        let epochs: Vec<_> = Epochs::new(&entries).collect();

        // 3 of the 10 ranges of the second epoch. The rule closes the full epoch, not the part.
        let part = epochs[1].part_after(16_999).unwrap();
        assert_eq!(part.entries(), &entries[17..20]);
        assert_eq!(part.previous(), Some(&entries[16]));
        assert_eq!(part.end_height(), 19_999);
        assert!(part.closed_by_rule());
        // The last epoch has 5 ranges: its part is not closed by the rule either.
        assert!(!epochs[2].part_after(21_999).unwrap().closed_by_rule());
        // At the end of the entry before the epoch, the part is the full epoch.
        assert_eq!(epochs[1].part_after(9_999), Some(epochs[1]));
    }

    #[test]
    fn a_part_after_the_epoch_end_or_another_height_is_none() {
        let entries = synthetic_entries(&[sapling(1_000); 25]);
        let epochs: Vec<_> = Epochs::new(&entries).collect();

        assert_eq!(epochs[1].part_after(19_999), None);
        assert_eq!(epochs[1].part_after(25_999), None);
        assert_eq!(epochs[1].part_after(4_999), None);
        assert_eq!(epochs[1].part_after(15_000), None);
        assert_eq!(epochs[0].part_after(0), None);
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
