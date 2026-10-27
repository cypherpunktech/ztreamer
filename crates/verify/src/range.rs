//! Checks of one range blob against its manifest entry.
//!
//! [`RangeBlob::check`] proves that a blob is the range that the manifest describes and that
//! its blocks form one chain. It does not rebuild the note commitment trees. The checks run in this
//! order:
//!
//! 1. The BLAKE3 digest and the length of the blob equal `blob_digest` and `blob_len`. If not,
//!    the blob was damaged in transfer: [`Reject::Refetch`].
//! 2. The format version of the blob equals `range_version`.
//! 3. The blob decodes to exactly [`RANGE_SIZE`] records from `start_height`.
//! 4. Each record points to the hash of the record before it. The first record points to the
//!    end hash of the previous range.
//! 5. Each record adds to the tree sizes exactly the commitments that it holds.
//! 6. The last hash and the last tree sizes equal `end_hash` and `end_tree_sizes`.
//!
//! A failure after step 1 means that the manifest entry and the blob agree, and that the range
//! is wrong: [`Reject::Rejected`].

use std::fmt;

use zakura_chain::{block, parameters::GENESIS_PREVIOUS_BLOCK_HASH};
use ztreamer_indexer::{
    codec::{CompactBlockRecord, RangeDecoder, TreeSizes},
    index::{
        RANGE_SIZE,
        range_meta::{BlobMismatch, ManifestEntry},
    },
};

/// The end of a checked range: what the next range must link to.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RangeLink {
    /// The hash of the last block.
    pub end_hash: block::Hash,
    /// The tree sizes after the last block.
    pub end_tree_sizes: TreeSizes,
}

impl RangeLink {
    /// The link before the genesis block: the genesis previous hash and empty trees.
    pub const GENESIS: Self = Self {
        end_hash: GENESIS_PREVIOUS_BLOCK_HASH,
        end_tree_sizes: TreeSizes { sapling: 0, orchard: 0, ironwood: 0 },
    };
}

impl From<&CompactBlockRecord> for RangeLink {
    fn from(record: &CompactBlockRecord) -> Self {
        Self { end_hash: record.hash, end_tree_sizes: record.end_tree_sizes }
    }
}

impl From<&ManifestEntry> for RangeLink {
    fn from(entry: &ManifestEntry) -> Self {
        Self { end_hash: entry.end_hash, end_tree_sizes: entry.end_tree_sizes }
    }
}

/// A check that a range can fail after its digest matches. [`Check::StartFrontier`] to
/// [`Check::SubtreeRoots`] are the checks of [`Epoch::verify`](crate::epoch::Epoch::verify).
/// The `Attested` checks are the checks of [`Epoch::attest`](crate::epoch::Epoch::attest).
/// [`Check::SampledBlock`] is the check of [`Epoch::sample`](crate::epoch::Epoch::sample).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Check {
    /// The blob format version is not `range_version`.
    RangeVersion,
    /// The blob does not decode, or a record is not at its height.
    Decode,
    /// The first record is not at `start_height`.
    StartHeight,
    /// The first record does not point to the end hash of the previous range.
    StartLink,
    /// A record does not point to the hash of the record before it.
    HashChain,
    /// A record does not add its own commitments to the tree sizes.
    TreeSizes,
    /// The last hash is not `end_hash`.
    EndHash,
    /// The tree sizes after the last record are not `end_tree_sizes`.
    EndTreeSizes,
    /// The start frontier of the epoch does not parse, or its tree sizes are not the end tree
    /// sizes of the range before the epoch.
    StartFrontier,
    /// A note commitment is not a field element of its pool, or the tree is full.
    Commitment,
    /// A rebuilt frontier at the end of a range is not the frontier of the manifest entry.
    Frontier,
    /// The subtrees that the rebuild completed in a range are not the `subtree_roots` of the
    /// manifest entry.
    SubtreeRoots,
    /// The node has another hash at the end height of a range.
    AttestedHash,
    /// The node has other tree roots at the end height of the epoch.
    AttestedRoots,
    /// The node has another root for a subtree that the epoch completed.
    AttestedSubtree,
    /// The record at a sampled height is not the record of the full block from the node.
    SampledBlock,
}

impl Check {
    /// The rejection of a range that failed this check at `height`.
    pub const fn at(self, height: u32) -> Reject {
        Reject::Rejected { check: self, height }
    }
}

/// Why a range was not accepted.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum Reject {
    /// The blob is not the one that the manifest describes. Fetch it again.
    #[error("range {start_height} does not match its manifest digest or length")]
    Refetch {
        /// The first height of the range.
        start_height: u32,
        /// The value that does not match: the length or the digest.
        #[source]
        mismatch: BlobMismatch,
    },
    /// The caller passed a different number of blobs than the epoch has entries.
    #[error("epoch has {entries} entries and {blobs} blobs")]
    BlobCount {
        /// The number of entries in the epoch.
        entries: usize,
        /// The number of blobs.
        blobs: usize,
    },
    /// The blob is the one that the manifest describes, and a check failed.
    #[error("range check {check:?} failed at height {height}")]
    Rejected {
        /// The check that failed.
        check: Check,
        /// The height where the check failed.
        height: u32,
    },
}

/// A range blob with the manifest entry that describes it.
#[derive(Clone, Copy)]
pub struct RangeBlob<'a> {
    bytes: &'a [u8],
    entry: &'a ManifestEntry,
}

impl<'a> RangeBlob<'a> {
    /// Pairs the bytes of a range blob with its manifest entry.
    pub const fn new(bytes: &'a [u8], entry: &'a ManifestEntry) -> Self {
        Self { bytes, entry }
    }

    /// Runs the checks in the module docs.
    ///
    /// `previous` is the end of the range before: [`RangeLink::GENESIS`] for the range at
    /// genesis. Returns the end of this range, for the check of the next range.
    pub fn check(&self, previous: &RangeLink) -> Result<RangeLink, Reject> {
        self.check_with(previous, |_| Ok(()))
    }

    /// [`RangeBlob::check`], and `visit` each record after its link and size checks pass.
    pub(crate) fn check_with(
        &self,
        previous: &RangeLink,
        mut visit: impl FnMut(&CompactBlockRecord) -> Result<(), Reject>,
    ) -> Result<RangeLink, Reject> {
        let entry = self.entry;
        // Do step 1 of the module docs: check the length and the digest.
        entry
            .check_blob(self.bytes)
            .map_err(|mismatch| Reject::Refetch { start_height: entry.start_height, mismatch })?;
        let start = entry.start_height;
        if self.bytes.first() != Some(&entry.range_version) {
            return Err(Check::RangeVersion.at(start));
        }
        let decoder = RangeDecoder::new(self.bytes).map_err(|_| Check::Decode.at(start))?;

        let mut link = *previous;
        for index in 0..RANGE_SIZE {
            let height = start.saturating_add(index);
            let record = decoder.record(index as usize).map_err(|_| Check::Decode.at(height))?;
            if record.height != height {
                return Err(Check::StartHeight.at(record.height));
            }
            if record.previous_hash != link.end_hash {
                let check = if index == 0 { Check::StartLink } else { Check::HashChain };
                return Err(check.at(height));
            }
            if link.end_tree_sizes.checked_add(record.additions()) != Some(record.end_tree_sizes) {
                return Err(Check::TreeSizes.at(height));
            }
            visit(&record)?;
            link = RangeLink::from(&record);
        }

        if link.end_hash != entry.end_hash {
            return Err(Check::EndHash.at(entry.end_height));
        }
        if link.end_tree_sizes != entry.end_tree_sizes {
            return Err(Check::EndTreeSizes.at(entry.end_height));
        }
        Ok(link)
    }
}

impl fmt::Debug for RangeBlob<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RangeBlob")
            .field("start_height", &self.entry.start_height)
            .field("len", &self.bytes.len())
            .finish_non_exhaustive()
    }
}

/// The manifest entry that describes `blob`, a range of `records`.
#[cfg(test)]
pub(crate) fn entry_for(records: &[CompactBlockRecord], blob: &[u8]) -> ManifestEntry {
    use ztreamer_indexer::index::range_meta::BlobDigest;

    let last = records.last().unwrap();
    ManifestEntry {
        range_version: blob[0],
        start_height: records[0].height,
        end_height: last.height,
        end_hash: last.hash,
        end_tree_sizes: last.end_tree_sizes,
        frontiers: ztreamer_indexer::index::range_meta::Frontiers::default(),
        subtree_roots: Vec::new(),
        blob_digest: BlobDigest::hash(blob),
        blob_len: blob.len() as u64,
        epoch_end: false,
    }
}

#[cfg(test)]
mod tests {
    use ztreamer_indexer::{
        codec::{RANGE_ENVELOPE_BYTES, encode_range},
        test_utils::{hash, mixed_records},
    };

    use super::*;

    /// A range blob of `records` in the given order, with the envelope of `original`. Unlike
    /// `encode_range`, it does not check the order or the hash chain.
    fn assemble(original: &[u8], records: &[CompactBlockRecord]) -> Vec<u8> {
        let (mut offsets, mut body) = (Vec::new(), Vec::new());
        for record in records {
            offsets.extend_from_slice(&(body.len() as u32).to_be_bytes());
            let bytes = record.encode().unwrap();
            body.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            body.extend_from_slice(&bytes);
        }
        offsets.extend_from_slice(&(body.len() as u32).to_be_bytes());
        [&original[..RANGE_ENVELOPE_BYTES], &offsets, &body].concat()
    }

    /// The records of the ranges at 0 and at 1,000, and the blob of the range at 1,000.
    fn second_range() -> (Vec<CompactBlockRecord>, Vec<CompactBlockRecord>, Vec<u8>) {
        let mut records = mixed_records(2 * RANGE_SIZE);
        let second = records.split_off(RANGE_SIZE as usize);
        let blob = encode_range(&second).unwrap();
        (records, second, blob)
    }

    fn assert_rejected(result: Result<RangeLink, Reject>, check: Check, height: u32) {
        assert_eq!(result, Err(Reject::Rejected { check, height }));
    }

    #[test]
    fn consecutive_ranges_pass() {
        let (first, second, blob) = second_range();
        let first_blob = encode_range(&first).unwrap();

        let link = RangeBlob::new(&first_blob, &entry_for(&first, &first_blob))
            .check(&RangeLink::GENESIS)
            .unwrap();
        let end = RangeBlob::new(&blob, &entry_for(&second, &blob)).check(&link).unwrap();

        assert_eq!(link, RangeLink::from(&entry_for(&first, &first_blob)));
        assert_eq!(end.end_hash, hash(1_999));
        assert_eq!(end.end_tree_sizes, second.last().unwrap().end_tree_sizes);
        assert!(end.end_tree_sizes.sapling > link.end_tree_sizes.sapling);
    }

    #[test]
    fn a_flipped_byte_or_a_wrong_length_needs_a_refetch() {
        let (first, second, blob) = second_range();
        let (entry, link) = (entry_for(&second, &blob), RangeLink::from(first.last().unwrap()));
        let refetch = |mismatch| Err(Reject::Refetch { start_height: 1_000, mismatch });

        let mut flipped = blob.clone();
        flipped[blob.len() / 2] ^= 1;
        assert_eq!(RangeBlob::new(&flipped, &entry).check(&link), refetch(BlobMismatch::Digest));
        let (expected, actual) = (blob.len() as u64, blob.len() as u64 - 1);
        assert_eq!(
            RangeBlob::new(&blob[..blob.len() - 1], &entry).check(&link),
            refetch(BlobMismatch::Len { expected, actual })
        );
    }

    #[test]
    fn a_reordered_block_is_rejected() {
        let (first, mut second, blob) = second_range();
        let link = RangeLink::from(first.last().unwrap());
        second.swap(500, 501);
        let reordered = assemble(&blob, &second);

        let result = RangeBlob::new(&reordered, &entry_for(&second, &reordered)).check(&link);

        assert_rejected(result, Check::Decode, 1_500);
    }

    #[test]
    fn a_broken_hash_chain_is_rejected() {
        let (first, mut second, blob) = second_range();
        let link = RangeLink::from(first.last().unwrap());
        second[500].previous_hash = hash(7);
        let broken = assemble(&blob, &second);

        let result = RangeBlob::new(&broken, &entry_for(&second, &broken)).check(&link);

        assert_rejected(result, Check::HashChain, 1_500);
    }

    #[test]
    fn a_wrong_start_link_is_rejected() {
        let (first, second, blob) = second_range();
        let entry = entry_for(&second, &blob);
        let link = RangeLink::from(first.last().unwrap());

        let other_hash = RangeLink { end_hash: hash(7), ..link };
        assert_rejected(RangeBlob::new(&blob, &entry).check(&other_hash), Check::StartLink, 1_000);
        // Without a previous range, the range must start at genesis.
        assert_rejected(
            RangeBlob::new(&blob, &entry).check(&RangeLink::GENESIS),
            Check::StartLink,
            1_000,
        );

        let mut other_start = entry.clone();
        other_start.start_height = 2_000;
        assert_rejected(
            RangeBlob::new(&blob, &other_start).check(&link),
            Check::StartHeight,
            1_000,
        );
    }

    /// Each row changes one field of the entry of an honest blob. The check must reject the
    /// blob at the expected check and height.
    #[test]
    fn a_wrong_entry_field_is_rejected() {
        type ChangeEntry = fn(&mut ManifestEntry);
        let cases: [(&str, ChangeEntry, Check, u32); 3] = [
            ("a wrong range version", |entry| entry.range_version = 2, Check::RangeVersion, 1_000),
            ("a wrong end hash", |entry| entry.end_hash = hash(7), Check::EndHash, 1_999),
            (
                "wrong end tree sizes",
                |entry| entry.end_tree_sizes.orchard += 1,
                Check::EndTreeSizes,
                1_999,
            ),
        ];
        let (first, second, blob) = second_range();
        let link = RangeLink::from(first.last().unwrap());

        for (case, change, check, height) in cases {
            let mut entry = entry_for(&second, &blob);
            change(&mut entry);

            let result = RangeBlob::new(&blob, &entry).check(&link);

            assert_eq!(result, Err(Reject::Rejected { check, height }), "{case}");
        }
    }

    #[test]
    fn wrong_tree_sizes_are_rejected() {
        let (first, mut second, blob) = second_range();
        let link = RangeLink::from(first.last().unwrap());

        let smaller =
            RangeLink { end_tree_sizes: TreeSizes { sapling: 0, ..link.end_tree_sizes }, ..link };
        let entry = entry_for(&second, &blob);
        assert_rejected(RangeBlob::new(&blob, &entry).check(&smaller), Check::TreeSizes, 1_000);

        // A record that claims one more commitment than it holds.
        for record in &mut second[500..] {
            record.end_tree_sizes.sapling += 1;
        }
        let blob = encode_range(&second).unwrap();
        let entry = entry_for(&second, &blob);
        assert_rejected(RangeBlob::new(&blob, &entry).check(&link), Check::TreeSizes, 1_500);
    }
}
