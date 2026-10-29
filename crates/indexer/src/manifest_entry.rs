//! What a snapshot manifest says about one sealed range.
//!
//! A [`ManifestEntry`] gives the end of a range, its tree frontiers, the subtrees that it
//! completed, and the length and the BLAKE3 digest of its blob. Its `epoch_end` flag follows the
//! epoch rule of [`MAX_RANGES`] and [`MAX_COMMITMENTS`]. Each `range_meta` row of the index keeps
//! one entry. The `wire` module converts an entry to and from the Replicator `ManifestEntry`
//! message.

use zakura_chain::block;

use crate::{
    codec::{CodecError, Reader, TreeSizes, put_bytes, take_bool},
    pool::{SubtreeRoot, put_subtree_roots, take_subtree_roots},
};

mod wire;
pub use wire::WireError;

/// The version of the manifest format. The JSON manifest of a snapshot and the Replicator
/// `ManifestEntry` message use this version.
pub const MANIFEST_VERSION: u32 = 1;

/// The maximum number of ranges in one verification epoch.
pub const MAX_RANGES: usize = 10;

/// A verification epoch closes at the first range that makes the epoch add this number of note
/// commitments, or more. The count includes all three pools.
pub const MAX_COMMITMENTS: u64 = 1_000_000;

/// The BLAKE3 digest of a range blob.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlobDigest(pub [u8; 32]);

impl BlobDigest {
    /// Calculates the BLAKE3 digest of `blob`.
    pub fn hash(blob: &[u8]) -> Self {
        Self(*blake3::hash(blob).as_bytes())
    }
}

/// Why a range blob does not match its [`ManifestEntry`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BlobMismatch {
    /// The length of the blob is not `blob_len`.
    #[error("the blob has {actual} bytes; the entry gives {expected}")]
    Len {
        /// The length that the entry gives.
        expected: u64,
        /// The length of the blob.
        actual: u64,
    },
    /// The BLAKE3 digest of the blob is not `blob_digest`.
    #[error("the blob does not have the digest that the entry gives")]
    Digest,
}

/// The three note commitment tree frontiers at the end of a range, each in the `zcashd` RPC
/// encoding. An empty frontier means the pool was not active at that height.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Frontiers {
    /// Sapling frontier.
    pub sapling: Vec<u8>,
    /// Orchard frontier.
    pub orchard: Vec<u8>,
    /// Ironwood frontier.
    pub ironwood: Vec<u8>,
}

impl Frontiers {
    /// Returns `true` if each pool that is active in `before` is also active in `self`.
    ///
    /// An empty frontier means that the pool is not active: `z_gettreestate` gives no tree for
    /// it. An active pool always has a frontier, also when its tree is empty. A pool does not
    /// become inactive, so `false` means that `self` is wrong.
    pub const fn follows(&self, before: &Self) -> bool {
        (before.sapling.is_empty() || !self.sapling.is_empty())
            && (before.orchard.is_empty() || !self.orchard.is_empty())
            && (before.ironwood.is_empty() || !self.ironwood.is_empty())
    }
}

/// What a snapshot manifest says about one sealed range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestEntry {
    /// The range blob format version.
    pub range_version: u8,
    /// The first height of the range.
    pub start_height: u32,
    /// The last height of the range.
    pub end_height: u32,
    /// The hash of the block at `end_height`.
    pub end_hash: block::Hash,
    /// The cumulative tree sizes after the block at `end_height`.
    pub end_tree_sizes: TreeSizes,
    /// The tree frontiers after the block at `end_height`.
    pub frontiers: Frontiers,
    /// The subtrees that the range completed.
    pub subtree_roots: Vec<SubtreeRoot>,
    /// The BLAKE3 digest of the range blob.
    pub blob_digest: BlobDigest,
    /// The length of the range blob in bytes.
    pub blob_len: u64,
    /// Whether the range closes a verification epoch.
    pub epoch_end: bool,
}

impl ManifestEntry {
    /// Checks that `blob` has the length and the BLAKE3 digest of the entry. It checks the length
    /// first, because the digest reads the full blob.
    pub fn check_blob(&self, blob: &[u8]) -> Result<(), BlobMismatch> {
        let actual = blob.len() as u64;
        if actual != self.blob_len {
            return Err(BlobMismatch::Len { expected: self.blob_len, actual });
        }
        (BlobDigest::hash(blob) == self.blob_digest).then_some(()).ok_or(BlobMismatch::Digest)
    }

    /// Makes an entry from the records of a range and from a range blob. For tests only.
    ///
    /// - `records` gives the start height (first record), and the end height, the end hash and
    ///   the end tree sizes (last record).
    /// - `blob` gives the range version (its first byte), the digest and the length.
    /// - The frontiers and the subtree roots are empty. `epoch_end` is false.
    ///
    /// `blob` can be different from the encoding of `records`. Thus a test can make an entry for
    /// a changed blob. To set other values, use struct update syntax.
    ///
    /// # Panics
    ///
    /// Panics if `records` or `blob` is empty.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn from_records_and_blob(
        records: &[crate::codec::CompactBlockRecord],
        blob: &[u8],
    ) -> Self {
        let last = records.last().unwrap();
        Self {
            range_version: blob[0],
            start_height: records[0].height,
            end_height: last.height,
            end_hash: last.hash,
            end_tree_sizes: last.end_tree_sizes,
            frontiers: Frontiers::default(),
            subtree_roots: Vec::new(),
            blob_digest: BlobDigest::hash(blob),
            blob_len: blob.len() as u64,
            epoch_end: false,
        }
    }

    pub(crate) fn encode_into(&self, bytes: &mut Vec<u8>) {
        bytes.push(self.range_version);
        bytes.extend_from_slice(&self.start_height.to_be_bytes());
        bytes.extend_from_slice(&self.end_height.to_be_bytes());
        bytes.extend_from_slice(&self.end_hash.0);
        for size in
            [self.end_tree_sizes.sapling, self.end_tree_sizes.orchard, self.end_tree_sizes.ironwood]
        {
            bytes.extend_from_slice(&size.to_be_bytes());
        }
        for frontier in [&self.frontiers.sapling, &self.frontiers.orchard, &self.frontiers.ironwood]
        {
            put_bytes(bytes, frontier);
        }
        put_subtree_roots(bytes, &self.subtree_roots);
        bytes.extend_from_slice(&self.blob_digest.0);
        bytes.extend_from_slice(&self.blob_len.to_be_bytes());
        bytes.push(u8::from(self.epoch_end));
    }

    pub(crate) fn decode_from(reader: &mut Reader<'_>) -> Result<Self, CodecError> {
        let range_version = reader.u8()?;
        let start_height = reader.u32()?;
        let end_height = reader.u32()?;
        let end_hash = block::Hash(reader.array()?);
        let end_tree_sizes =
            TreeSizes { sapling: reader.u32()?, orchard: reader.u32()?, ironwood: reader.u32()? };
        let frontiers = Frontiers {
            sapling: reader.bytes()?.to_vec(),
            orchard: reader.bytes()?.to_vec(),
            ironwood: reader.bytes()?.to_vec(),
        };
        let subtree_roots = take_subtree_roots(reader)?;
        let blob_digest = BlobDigest(reader.array()?);
        let blob_len = reader.u64()?;
        let epoch_end = take_bool(reader)?;
        Ok(Self {
            range_version,
            start_height,
            end_height,
            end_hash,
            end_tree_sizes,
            frontiers,
            subtree_roots,
            blob_digest,
            blob_len,
            epoch_end,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::hash;

    #[test]
    fn check_blob_compares_the_length_then_the_digest() {
        let blob = b"range blob";
        let entry = ManifestEntry {
            blob_digest: BlobDigest::hash(blob),
            blob_len: blob.len() as u64,
            range_version: 1,
            start_height: 0,
            end_height: 999,
            end_hash: hash(999),
            end_tree_sizes: TreeSizes::default(),
            frontiers: Frontiers::default(),
            subtree_roots: Vec::new(),
            epoch_end: false,
        };
        assert_eq!(entry.check_blob(blob), Ok(()));
        assert_eq!(
            entry.check_blob(&blob[1..]),
            Err(BlobMismatch::Len { expected: 10, actual: 9 })
        );
        assert_eq!(entry.check_blob(b"other blob"), Err(BlobMismatch::Digest));
    }

    #[test]
    fn a_pool_that_becomes_inactive_does_not_follow() {
        let active = Frontiers { sapling: vec![1], orchard: vec![0, 0, 0], ironwood: Vec::new() };
        let mut later = Frontiers { ironwood: vec![2], ..active.clone() };
        assert!(later.follows(&active));
        assert!(active.follows(&Frontiers::default()));

        later.orchard.clear();
        assert!(!later.follows(&active));
    }
}
