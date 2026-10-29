//! [`IndexError`]: why an index operation failed.

use std::io;

use super::range_meta::BlobMismatch;
use crate::codec::CodecError;

#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Heed(#[from] heed::Error),
    #[error(transparent)]
    Codec(#[from] CodecError),
    #[error("LMDB network, genesis hash, or fixed index policy does not match")]
    Identity,
    #[error("LMDB metadata is incomplete or inconsistent")]
    Metadata,
    #[error("write batch was built from stale index generation {batch_generation}")]
    StaleBatch { batch_generation: u64 },
    #[error("mutable range starting at {start} is incomplete")]
    IncompleteRange { start: u32 },
    #[error("LMDB compact coverage is not contiguous at height {height}")]
    Continuity { height: u32 },
    #[error("LMDB hash index does not match compact block at height {height}")]
    HashIndex { height: u32 },
    #[error("serving snapshot generation {expected} does not match LMDB generation {actual}")]
    Generation { expected: u64, actual: u64 },
    #[error("compact block height {height} is outside indexed coverage")]
    Coverage { height: u32 },
    #[error("height or generation overflow")]
    Overflow,
    #[error("ordinary reorg replacement would modify sealed block {height}")]
    Sealed { height: u32 },
    #[error("replacement suffix does not connect at height {height}")]
    Replacement { height: u32 },
    #[error("range metadata at {start} does not describe a sealed range that starts there")]
    RangeMeta { start: u32 },
    /// The canonical block at the end height of a range does not have the end hash of the range
    /// metadata. A deep reorg replaced the range.
    #[error("sealed range at {start} does not end with the end hash of its metadata")]
    RangeEndHash {
        /// The first height of the range.
        start: u32,
    },
    #[error("height {height} is not the first height of a range")]
    RangeStart { height: u32 },
    #[error("sealed range at {start} is not the next range; the next range starts at {next}")]
    RangeOrder { start: u32, next: u32 },
    #[error("range blob does not continue the chain at height {height}")]
    RangeBlob { height: u32 },
    #[error("block hash at height {height} is in the index already")]
    DuplicateHash { height: u32 },
    #[error("index has mutable rows; an install needs an index with no mutable rows")]
    MutableRows,
    /// A range blob does not have the `blob_len` or the `blob_digest` of its metadata.
    #[error("range blob at {start} does not match its metadata: {mismatch}")]
    Blob {
        /// The first height of the range.
        start: u32,
        /// The value that does not match: the length or the digest.
        mismatch: BlobMismatch,
    },
}
