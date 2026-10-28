//! Generation-pinned LMDB reads, atomic writes, packed ranges, and reorg replacement.
//!
//! # Tables
//!
//! The [`Index`] keeps the chain in five LMDB databases in one environment. All integers are
//! big-endian, so LMDB keeps the height keys in height order.
//!
//! ```text
//! table           key                    value
//! metadata        "state"                IndexState, 62 or 67 bytes
//!                 "identity"             chain identity and fixed policy
//! sealed_ranges   first height, u32      one packed range of RANGE_SIZE records
//! mutable_blocks  height, u32            one encoded CompactBlockRecord
//! hash_to_height  block hash, 32 bytes   height, u32
//! range_meta      first height, u32      one RangeMeta record for a sealed range
//! ```
//!
//! # Coverage
//!
//! Each height from 0 to the durable tip is in exactly one of `sealed_ranges` and
//! `mutable_blocks`. The sealed ranges hold the old blocks. The mutable rows hold the newer
//! blocks. The newest blocks are not in LMDB. For example, with the node tip at 2,544:
//!
//! ```text
//! height   0 ... 999 | 1000 ... 1999 | 2000 ... 2534  | 2535 ... 2544
//! table    sealed_ranges             | mutable_blocks | not in LMDB
//! key      0         | 1000          | 2000, 2001 ... |
//!                                  ^               ^                ^
//!                     sealed_through     durable tip         node tip
//!
//! hash_to_height: one entry for each height in 0..=2534
//! ```
//!
//! The caller selects the blocks to write and the ranges to seal. The head follower writes a
//! block when it is [`PERSIST_DEPTH`] or more blocks below the node tip. It seals a range when
//! the last block of the range is [`SEAL_DEPTH`] or more blocks below the node tip. In the
//! example, the range 2,000 to 2,999 is not complete, so it stays in `mutable_blocks`.
//!
//! A deep reorg can go below `sealed_through`. Then [`Index::replace_deep_suffix`] deletes the
//! affected ranges, writes their blocks again as mutable rows, and seals the complete ranges
//! again.
//!
//! A replica gets its sealed ranges from a snapshot, not from a node.
//! [`Index::install_sealed_range`] writes one packed range and its `range_meta` row. It accepts
//! ranges only in height order, with no gaps. [`Index::install_checked_range`] does the same
//! install for a [`CheckedRange`]: the checks of the blob that do not need the index ran before,
//! on another thread. During an import, the index has no mutable rows.
//! Thus the durable tip is `sealed_through`. [`Index::read_sealed_range_bytes`] reads the packed
//! range that a snapshot gives.
//!
//! # Rules
//!
//! These rules apply after each commit:
//!
//! - `sealed_through` is not set, or it is the last height of a range. The sealed ranges cover
//!   `0..=sealed_through` with no gaps.
//! - The mutable rows cover `sealed_through + 1` to the durable tip, one row for each height.
//! - `hash_to_height` has one entry for each height from 0 to the durable tip, and no other
//!   entries.
//! - The previous hash of each block is the hash of the block below it.
//! - The state holds the hash and the tree sizes of the durable tip.
//! - `verified_through` is not set, or it is the last height of a range at or below
//!   `sealed_through`. Only these writes advance it:
//!   - An install of a [`VerifyState::Verified`](range_meta::VerifyState::Verified) range at
//!     `verified_through + 1`, or at 0.
//!   - A seal of mutable rows into a range at `verified_through + 1`.
//!
//!   A seal does not set a `verified_through` that is not set. Thus the embedded mode never
//!   sets it.
//! - Every `range_meta` key is the first height of a sealed range. A sealed range may have no
//!   `range_meta` row.
//! - An empty index has no state row, and its block tables and `range_meta` are empty.
//!
//! # Writes and generations
//!
//! Each write is one LMDB write transaction. It changes the tables, then it commits. If a write
//! fails, LMDB discards all of its changes. Each commit increases the generation by 1, except
//! [`Index::put_range_meta`]: range metadata is not part of what a reader sees at a generation,
//! so writing it does not invalidate the readers of the current generation. An install writes a
//! `range_meta` row and a range in one commit. Thus an install increases the generation.
//!
//! An LMDB read transaction sees one committed state. The `read_*` methods take a generation, and
//! they return [`IndexError::Generation`] if the index has a different generation. Thus a reader
//! cannot mix blocks from two commits. [`Index::read_range_latest`] reads the current generation.
//! Its caller checks the chain between two calls.
//!
//! # Startup checks
//!
//! [`Index::open`] rejects an environment with a different identity: a different network, genesis
//! hash, [`SCHEMA_VERSION`], [`RANGE_SIZE`], [`PERSIST_DEPTH`] or [`SEAL_DEPTH`]. Then
//! [`Index::verify_continuity`] checks:
//!
//! - the number of rows in each block table,
//! - that the height keys of `sealed_ranges` and `mutable_blocks` start at 0 and have no gaps,
//! - the links between ranges, and the hash chain of the mutable rows,
//! - the hash entry of each mutable row, and of the first and the last block of each range,
//! - the tip hash and the tree sizes in the state,
//! - that each `range_meta` key is the start of a sealed range.
//!
//! It does not decode the blocks inside a range. Thus it does not examine their chain links or
//! their hash entries. The range encoder checks the chain links when it writes the range. An
//! install, or the [`CheckedRange::check`] before it, decodes each block of the range and checks
//! the chain links. The install writes each hash entry.
//!
//! # Value formats
//!
//! - A mutable row is the encoding of one [`CompactBlockRecord`](crate::codec::CompactBlockRecord):
//!   `version (u8) | height | hash (32 bytes) | previous hash (32 bytes) | length | protobuf
//!   CompactBlock`.
//! - A sealed range holds [`RANGE_SIZE`] records in the same encoding, and an offset table. A
//!   reader can decode one record and not the others. [`encode_range`](crate::codec::encode_range)
//!   writes a range. The `write_range` function in `codec.rs` gives its byte layout.
//! - `index/metadata.rs` gives the byte layout of the state and of the identity, and
//!   `index/range_meta.rs` the layout of a range metadata record.
//!
//! # Files
//!
//! The methods of [`Index`] are in one file for each operation: `index/read.rs` reads rows,
//! ranges and blocks, `index/install.rs` installs checked ranges, `index/write.rs` writes blocks
//! and seals ranges, and `index/reorg.rs` replaces a suffix after a reorg.

use std::{fs, io, path::Path};

use heed::{Env, EnvOpenOptions, RoTxn, RwTxn};
use zakura_chain::{block, parameters::GENESIS_PREVIOUS_BLOCK_HASH};

use crate::codec::{CodecError, TreeSizes};

mod checked_range;
mod install;
mod metadata;
pub mod range_meta;
mod read;
mod reorg;
mod tables;
mod telemetry;
#[cfg(test)]
mod test_support;
mod write;
mod writer;
pub use checked_range::{CheckedRange, RangeCheckError};
use metadata::Metadata;
use range_meta::{BlobMismatch, RangeMeta};
use tables::Tables;
use telemetry::record_state_metrics;

pub const SCHEMA_VERSION: u32 = 1;
pub const RANGE_SIZE: u32 = 1_000;
pub const PERSIST_DEPTH: u32 = 10;
pub const SEAL_DEPTH: u32 = 100;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// References a block by hash and height.
pub struct BlockId {
    pub height: u32,
    pub hash: block::Hash,
}

impl BlockId {
    pub const fn new(height: u32, hash: block::Hash) -> Self {
        Self { height, hash }
    }
}

/// The chain of an index, as its identity row stores it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChainIdentity {
    /// The network name that the index was opened with, for example `Mainnet`.
    pub network: String,
    /// The hash of the genesis block.
    pub genesis_hash: block::Hash,
}

/// The metadata row and the packed blob of one sealed range, from one LMDB snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedRangeWithMeta {
    /// The `range_meta` row of the range.
    pub meta: RangeMeta,
    /// The packed blob of the range, as in `sealed_ranges`.
    pub blob: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
/// Represents the [`Index`]'s current sync state.
pub struct IndexState {
    /// The highest contiguous compact block committed to the `mutable_blocks` database.
    pub(crate) durable_tip: Option<BlockId>,
    /// The highest height committed to the `sealed_ranges` database.
    ///
    /// Blocks above this height live in the `mutable_blocks` database.
    pub(crate) sealed_through: Option<u32>,
    /// The highest height through which every sealed range is verified against a node.
    ///
    /// Embedded mode never sets it. It is at most `sealed_through`.
    pub(crate) verified_through: Option<u32>,
    /// LMDB's monotonically increasing revision number.
    /// Incremented on each successful atomic index mutation.
    pub(crate) generation: u64,
    /// Cumulative [`TreeSizes`] at the end of `durable_tip`
    pub(crate) tree_sizes: TreeSizes,
}

impl IndexState {
    /// Returns the highest contiguous compact block committed to the
    /// `mutable_blocks` database.
    pub const fn durable_tip(&self) -> Option<BlockId> {
        self.durable_tip
    }

    /// Returns the highest height committed to the `sealed_ranges` database.
    ///
    /// Blocks above this height live in the `mutable_blocks` database.
    pub const fn sealed_through(&self) -> Option<u32> {
        self.sealed_through
    }

    /// Returns the highest height through which every sealed range is verified against a node,
    /// or `None` when no range is.
    pub const fn verified_through(&self) -> Option<u32> {
        self.verified_through
    }

    /// Identifies the exact committed revision of the durable index that an
    /// [`IndexState`] describes.
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Cumulative [`TreeSizes`] at the end of [`IndexState::durable_tip`]
    pub const fn tree_sizes(&self) -> TreeSizes {
        self.tree_sizes
    }

    /// Returns the first height of the next range to seal: `sealed_through + 1`, or 0 if no range
    /// is sealed.
    pub(crate) fn next_range_start(&self) -> Result<u32, IndexError> {
        self.sealed_through
            .map_or(Ok(0), |height| height.checked_add(1).ok_or(IndexError::Overflow))
    }

    /// Returns the first height of the next range to verify: `verified_through + 1`, or 0 if no
    /// range is verified.
    pub(crate) fn next_verify_start(&self) -> Result<u32, IndexError> {
        self.verified_through
            .map_or(Ok(0), |height| height.checked_add(1).ok_or(IndexError::Overflow))
    }

    /// The hash that the first block of an installed range links to: the hash of the tip, or the
    /// genesis previous hash on an empty index. An install requires no mutable rows. Thus the
    /// tip is the last block of the sealed ranges.
    pub(crate) fn install_parent_hash(&self) -> block::Hash {
        self.durable_tip.map_or(GENESIS_PREVIOUS_BLOCK_HASH, |tip| tip.hash)
    }

    /// Returns `true` if the index has mutable rows. The mutable rows cover the heights above
    /// `sealed_through` to the durable tip. Thus they exist only if the tip is above
    /// `sealed_through`.
    pub(crate) fn has_mutable_rows(&self) -> bool {
        self.durable_tip.map(|tip| tip.height) != self.sealed_through
    }
}

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

/// Ztreamer's five-database LMDB index.
pub struct Index {
    env: Env,
    tables: Tables,
}

impl std::fmt::Debug for Index {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Index").field("path", &self.env.path()).finish_non_exhaustive()
    }
}

impl Index {
    /// Opens the index and rejects an environment created for another chain or format.
    pub fn open(
        path: impl AsRef<Path>,
        map_size: usize,
        network: &str,
        genesis_hash: block::Hash,
    ) -> Result<Self, IndexError> {
        fs::create_dir_all(path.as_ref())?;
        Self::open_with(path.as_ref(), map_size, |metadata, txn| {
            metadata.put_or_check_identity(txn, network, genesis_hash)
        })
    }

    /// Opens an index that exists, for the chain in its identity row. A tool that does not know
    /// the chain of the index uses it, for example `ztreamerd export-snapshot`. Refuses a path
    /// with no index, and an index with another format. [`Index::identity`] gives the chain.
    pub fn open_existing(path: impl AsRef<Path>, map_size: usize) -> Result<Self, IndexError> {
        let path = path.as_ref();
        if !path.join("data.mdb").is_file() {
            let message = format!("{} holds no index", path.display());
            return Err(io::Error::new(io::ErrorKind::NotFound, message).into());
        }
        Self::open_with(path, map_size, |metadata, txn| metadata.get_identity(txn).map(drop))
    }

    /// The chain of the index, as its identity row stores it.
    pub fn identity(&self) -> Result<ChainIdentity, IndexError> {
        let txn = self.env.read_txn()?;
        self.tables.metadata.get_identity(&txn)
    }

    /// Opens the environment and its tables, runs `check` on the identity, and checks the
    /// coverage.
    fn open_with(
        path: &Path,
        map_size: usize,
        check: impl FnOnce(&Metadata, &mut RwTxn<'_>) -> Result<(), IndexError>,
    ) -> Result<Self, IndexError> {
        // SAFETY: callers must not open this path with incompatible LMDB options in this process.
        let env = unsafe { EnvOpenOptions::new().map_size(map_size).max_dbs(5).open(path)? };
        let mut txn = env.write_txn()?;
        let tables = Tables::open(&env, &mut txn)?;
        check(&tables.metadata, &mut txn)?;
        txn.commit()?;

        let index = Self { env, tables };
        index.verify_continuity()?;
        record_state_metrics(index.state()?);
        Ok(index)
    }

    pub fn state(&self) -> Result<IndexState, IndexError> {
        let txn = self.env.read_txn()?;
        self.tables.metadata.get_state(&txn)
    }

    /// Returns the index state. If the index is not at `generation`, returns
    /// [`IndexError::Generation`].
    pub fn state_at(&self, generation: u64) -> Result<IndexState, IndexError> {
        let txn = self.env.read_txn()?;
        self.read_generation(&txn, generation)
    }

    fn read_generation(&self, txn: &RoTxn<'_>, generation: u64) -> Result<IndexState, IndexError> {
        let state = self.tables.metadata.get_state(txn)?;
        if state.generation != generation {
            return Err(IndexError::Generation { expected: generation, actual: state.generation });
        }
        Ok(state)
    }
}

#[cfg(test)]
mod tests {
    use super::{test_support::*, *};

    use crate::index::range_meta::full_meta;

    /// An index environment as a build before the `range_meta` table left it: four named
    /// databases and the identity row, and nothing else.
    #[test]
    fn opens_an_index_created_before_range_meta() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index");
        fs::create_dir_all(&path).unwrap();
        let identity = [
            SCHEMA_VERSION.to_be_bytes().as_slice(),
            RANGE_SIZE.to_be_bytes().as_slice(),
            PERSIST_DEPTH.to_be_bytes().as_slice(),
            SEAL_DEPTH.to_be_bytes().as_slice(),
            [1; 32].as_slice(),
            b"Mainnet",
        ]
        .concat();
        {
            // SAFETY: the test owns this directory.
            let env = unsafe {
                EnvOpenOptions::new().map_size(10 * 1024 * 1024).max_dbs(4).open(&path).unwrap()
            };
            let mut txn = env.write_txn().unwrap();
            let metadata = env
                .create_database::<heed::types::Bytes, heed::types::Bytes>(
                    &mut txn,
                    Some("metadata"),
                )
                .unwrap();
            for name in ["sealed_ranges", "mutable_blocks", "hash_to_height"] {
                env.create_database::<heed::types::Bytes, heed::types::Bytes>(&mut txn, Some(name))
                    .unwrap();
            }
            metadata.put(&mut txn, b"identity", identity.as_slice()).unwrap();
            txn.commit().unwrap();
        }

        let index = Index::open(&path, 10 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap();
        assert_eq!(index.state().unwrap(), IndexState::default());
        assert_eq!(index.range_meta(0).unwrap(), None);
        let state = index.write(batch(&index, 0..=1_100, 1_200)).unwrap();
        assert_eq!(state.sealed_through(), Some(999));
        assert_eq!(state.verified_through(), None);
        index.put_range_meta(0, &full_meta(0)).unwrap();
        assert_eq!(index.range_meta(0).unwrap(), Some(full_meta(0)));
    }

    #[test]
    fn creates_the_databases_and_checks_chain_identity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index");

        drop(Index::open(&path, 10 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap());
        drop(Index::open(&path, 10 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap());
        assert!(Index::open(&path, 10 * 1024 * 1024, "Testnet", block::Hash([1; 32])).is_err());
        assert!(Index::open(&path, 10 * 1024 * 1024, "Mainnet", block::Hash([2; 32])).is_err());
    }
}
