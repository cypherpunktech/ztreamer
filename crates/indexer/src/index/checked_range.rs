//! Range blobs that passed the checks of an install that do not need the index.
//!
//! [`Index::install_sealed_range`](super::Index::install_sealed_range) checks a blob in two
//! parts:
//!
//! 1. The checks that need only the blob and its manifest entry: the length and the BLAKE3
//!    digest, the envelope, the decode of each record, the rules of
//!    [`CompactBlockRecord::encode`], and the links and the tree sizes from each record to the
//!    next record.
//! 2. The checks that need the index: the order of the install, no mutable rows, the link and
//!    the tree sizes of the first record against the tip, the `range_meta` row against the blob,
//!    and no duplicate block hash.
//!
//! [`CheckedRange::check`] does part 1, for example on a thread pool before the install.
//! [`Index::install_checked_range`](super::Index::install_checked_range) then does only part 2.
//! It does not calculate the digest and does not decode a record. The serial install thus
//! does not do part 1 again.
//!
//! Only [`CheckedRange::check`] and [`CheckedRange::check_with`] make a [`CheckedRange`]. Thus
//! each value is a proof that the indexer did part 1 on its bytes. [`Bytes`] is immutable,
//! thus the bytes cannot change after the check.

use std::{convert::Infallible, fmt};

use prost::bytes::Bytes;

use super::{
    BlockId, IndexError,
    range_meta::{BlobDigest, BlobMismatch, ManifestEntry},
    tables::{RangeChain, SealedRange},
};
use crate::codec::{CodecError, CompactBlockRecord};

/// A range blob that passed the checks of an install that do not need the index. The module
/// docs give the checks.
pub struct CheckedRange {
    blob: Bytes,
    blob_digest: BlobDigest,
    start: u32,
    chain: RangeChain,
}

impl CheckedRange {
    /// Checks `blob` against `entry` and the rules of a sealed range. Part 1 of the module docs
    /// gives the checks, in their order.
    pub fn check(blob: Bytes, entry: &ManifestEntry) -> Result<Self, RangeCheckError<Infallible>> {
        Self::check_with(blob, entry, |_| Ok(()))
    }

    /// [`CheckedRange::check`], and `visit` each record in height order. `visit` gets a record
    /// after its decode and the rules of [`CompactBlockRecord::encode`], and before the check of
    /// its link to the record below it. Thus a caller can do stricter checks first, with its own
    /// errors. An error of `visit` stops the check.
    pub fn check_with<E>(
        blob: Bytes,
        entry: &ManifestEntry,
        visit: impl FnMut(&CompactBlockRecord) -> Result<(), E>,
    ) -> Result<Self, RangeCheckError<E>> {
        entry.check_blob(&blob).map_err(RangeCheckError::Blob)?;
        let range = SealedRange::from_envelope(&blob).map_err(RangeCheckError::Envelope)?;
        let (start, chain) = (range.start(), range.check_chain_with(None, visit)?);
        Ok(Self { blob, blob_digest: entry.blob_digest, start, chain })
    }

    /// The first height of the range, from the envelope of the blob.
    pub const fn start(&self) -> u32 {
        self.start
    }

    /// The bytes of the blob.
    pub const fn blob(&self) -> &Bytes {
        &self.blob
    }

    /// Checks that `entry` gives the length and the digest of the blob. It checks the length
    /// first, as [`ManifestEntry::check_blob`] does. It does not calculate the digest again.
    pub(crate) fn check_entry_blob(&self, entry: &ManifestEntry) -> Result<(), BlobMismatch> {
        let actual = self.blob.len() as u64;
        if entry.blob_len != actual {
            return Err(BlobMismatch::Len { expected: entry.blob_len, actual });
        }
        (entry.blob_digest == self.blob_digest).then_some(()).ok_or(BlobMismatch::Digest)
    }

    /// The id of each block, the first link and the last record.
    pub(crate) const fn chain(&self) -> &RangeChain {
        &self.chain
    }
}

impl AsRef<[u8]> for CheckedRange {
    fn as_ref(&self) -> &[u8] {
        &self.blob
    }
}

impl fmt::Debug for CheckedRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let last = &self.chain.last;
        f.debug_struct("CheckedRange")
            .field("start", &self.start)
            .field("len", &self.blob.len())
            .field("end", &BlockId::new(last.height, last.hash))
            .finish_non_exhaustive()
    }
}

/// Why a blob is not a [`CheckedRange`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RangeCheckError<E> {
    /// The blob does not have the length or the digest of the entry.
    #[error("blob does not match its manifest entry")]
    Blob(#[source] BlobMismatch),
    /// The envelope or the offset table of the blob does not decode.
    #[error("range envelope does not decode")]
    Envelope(#[source] CodecError),
    /// The record at `height` does not decode, or it does not obey the rules of
    /// [`CompactBlockRecord::encode`].
    #[error("record {height} does not decode")]
    Record {
        /// The height of the record: the range start plus its position.
        height: u32,
        /// Why the record does not decode.
        #[source]
        source: CodecError,
    },
    /// The record at `height` does not link to the record below it, or its tree sizes are
    /// smaller.
    #[error("record {height} does not continue the chain")]
    Chain {
        /// The height of the record.
        height: u32,
    },
    /// `visit` refused the record.
    #[error(transparent)]
    Visit(E),
}

impl RangeCheckError<Infallible> {
    /// The error of an install of the range at `start` that failed this check.
    pub(crate) fn into_index_error(self, start: u32) -> IndexError {
        match self {
            Self::Blob(mismatch) => IndexError::Blob { start, mismatch },
            Self::Envelope(source) | Self::Record { source, .. } => source.into(),
            Self::Chain { height } => IndexError::RangeBlob { height },
            Self::Visit(never) => match never {},
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        codec::{TreeSizes, encode_range, encode_range_from_bytes},
        index::RANGE_SIZE,
        test_utils::{encode_with_unshielded_transaction, hash, mixed_records},
    };

    /// The records of the range at 0, and their blob.
    fn first_range() -> (Vec<CompactBlockRecord>, Bytes) {
        let records = mixed_records(RANGE_SIZE);
        let blob = encode_range(&records).unwrap().into();
        (records, blob)
    }

    /// `blob` with the encoding of the record at `height` changed by `change`. The encoding
    /// keeps its length. Thus the range encoder does not see the change.
    fn with_changed_record(
        blob: &[u8],
        records: &[CompactBlockRecord],
        height: usize,
        change: impl FnOnce(&mut CompactBlockRecord),
    ) -> Bytes {
        let good = records[height].encode().unwrap();
        let mut bad = records[height].clone();
        change(&mut bad);
        let bad = bad.encode().unwrap();
        assert_eq!(bad.len(), good.len());
        let mut blob = blob.to_vec();
        let at = blob.windows(good.len()).position(|bytes| bytes == good).unwrap();
        blob[at..at + good.len()].copy_from_slice(&bad);
        blob.into()
    }

    /// Checks `blob` against an entry that gives its length and digest.
    fn check(
        records: &[CompactBlockRecord],
        blob: Bytes,
    ) -> Result<CheckedRange, RangeCheckError<Infallible>> {
        let entry = ManifestEntry::from_records_and_blob(records, &blob);
        CheckedRange::check(blob, &entry)
    }

    #[test]
    fn a_range_checks_and_keeps_its_chain() {
        let (records, blob) = first_range();
        let entry = ManifestEntry::from_records_and_blob(&records, &blob);
        let mut visited = Vec::new();

        let range = CheckedRange::check_with(blob.clone(), &entry, |record| {
            visited.push(record.height);
            Ok::<_, Infallible>(())
        })
        .unwrap();

        assert_eq!(visited, (0..RANGE_SIZE).collect::<Vec<_>>());
        assert_eq!((range.start(), range.blob()), (0, &blob));
        assert_eq!(range.check_entry_blob(&entry), Ok(()));
        let chain = range.chain();
        assert_eq!(chain.ids.len(), RANGE_SIZE as usize);
        assert_eq!(chain.ids[500], BlockId::new(500, hash(500)));
        assert_eq!(chain.first.previous_hash, records[0].previous_hash);
        assert_eq!(chain.first.tree_sizes, records[0].end_tree_sizes);
        assert_eq!(chain.last, records[999]);
    }

    #[test]
    fn a_blob_that_is_not_the_entry_blob_is_refused_before_the_decode() {
        let (records, blob) = first_range();
        let entry = ManifestEntry::from_records_and_blob(&records, &blob);

        let short = blob.slice(..blob.len() - 1);
        let (expected, actual) = (blob.len() as u64, blob.len() as u64 - 1);
        assert_eq!(
            CheckedRange::check(short, &entry).unwrap_err(),
            RangeCheckError::Blob(BlobMismatch::Len { expected, actual })
        );
        let mut flipped = blob.to_vec();
        flipped[0] ^= 1;
        assert_eq!(
            CheckedRange::check(flipped.into(), &entry).unwrap_err(),
            RangeCheckError::Blob(BlobMismatch::Digest)
        );

        // The entry also binds the proof: another entry gives another length or digest.
        let range = CheckedRange::check(blob, &entry).unwrap();
        let other = ManifestEntry { blob_digest: BlobDigest::hash(b"other"), ..entry.clone() };
        assert_eq!(range.check_entry_blob(&other), Err(BlobMismatch::Digest));
        let longer = ManifestEntry { blob_len: entry.blob_len + 1, ..entry };
        assert!(matches!(range.check_entry_blob(&longer), Err(BlobMismatch::Len { .. })));
    }

    #[test]
    fn a_range_that_does_not_decode_is_refused() {
        let (records, blob) = first_range();

        // Another range format version.
        let mut version = blob.to_vec();
        version[0] = 2;
        assert!(matches!(
            check(&records, version.into()),
            Err(RangeCheckError::Envelope(CodecError::Version { .. }))
        ));

        // A transaction without shielded data at 500.
        let mut parts: Vec<_> = records.iter().map(|record| record.encode().unwrap()).collect();
        parts[500] = encode_with_unshielded_transaction(&records[500]);
        let parts: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
        let unshielded = encode_range_from_bytes(&parts).unwrap();
        assert_eq!(
            check(&records, unshielded.into()).unwrap_err(),
            RangeCheckError::Record { height: 500, source: CodecError::InvalidRecord }
        );
    }

    #[test]
    fn a_record_that_does_not_continue_the_chain_is_refused() {
        let (records, blob) = first_range();

        let unlinked = with_changed_record(&blob, &records, 500, |record| {
            record.previous_hash = block_hash(9);
        });
        assert_eq!(check(&records, unlinked).unwrap_err(), RangeCheckError::Chain { height: 500 });
        // The range encoder does not check the tree sizes.
        let mut smaller = records.clone();
        smaller[500].end_tree_sizes = TreeSizes::default();
        assert!(records[499].end_tree_sizes.total() > 0);
        let smaller = encode_range(&smaller).unwrap().into();
        assert_eq!(check(&records, smaller).unwrap_err(), RangeCheckError::Chain { height: 500 });

        // The first record has no block below it in the blob. The install checks its link.
        let mut first_unlinked = records.clone();
        first_unlinked[0].previous_hash = block_hash(9);
        let range = check(&records, encode_range(&first_unlinked).unwrap().into()).unwrap();
        assert_eq!(range.chain().first.previous_hash, block_hash(9));
    }

    #[test]
    fn a_visit_error_stops_the_check_before_the_link_check() {
        let (records, blob) = first_range();
        let entry = ManifestEntry::from_records_and_blob(&records, &blob);
        let unlinked = with_changed_record(&blob, &records, 500, |record| {
            record.previous_hash = block_hash(9);
        });
        let entry_unlinked = ManifestEntry::from_records_and_blob(&records, &unlinked);
        let refuse_at_500 = |record: &CompactBlockRecord| match record.height {
            500 => Err("refused"),
            _ => Ok(()),
        };

        assert_eq!(
            CheckedRange::check_with(blob, &entry, refuse_at_500).unwrap_err(),
            RangeCheckError::Visit("refused")
        );
        assert_eq!(
            CheckedRange::check_with(unlinked, &entry_unlinked, refuse_at_500).unwrap_err(),
            RangeCheckError::Visit("refused")
        );
    }

    fn block_hash(byte: u8) -> zakura_chain::block::Hash {
        zakura_chain::block::Hash([byte; 32])
    }
}
