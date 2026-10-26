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
//! ranges only in height order, with no gaps. During an import, the index has no mutable rows.
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
//!   `sealed_through`. Only an install of a [`VerifyState::Verified`] range advances it. The
//!   range must start at `verified_through + 1`, or at 0.
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
//! install decodes each block of the range. It checks the chain links and writes each hash entry.
//!
//! # Value formats
//!
//! - A mutable row is the encoding of one [`CompactBlockRecord`]:
//!   `version (u8) | height | hash (32 bytes) | previous hash (32 bytes) | length | protobuf
//!   CompactBlock`.
//! - A sealed range holds [`RANGE_SIZE`] records in the same encoding, and an offset table. A
//!   reader can decode one record and not the others. [`encode_range`](crate::codec::encode_range)
//!   writes a range. The `write_range` function in `codec.rs` gives its byte layout.
//! - `index/metadata.rs` gives the byte layout of the state and of the identity, and
//!   `index/range_meta.rs` the layout of a range metadata record.

use std::{fs, io, path::Path, time::Instant};

use heed::{Env, EnvOpenOptions, RoTxn, RwTxn};
use zakura_chain::{block, parameters::GENESIS_PREVIOUS_BLOCK_HASH};

use crate::codec::{
    CodecError, CompactBlockRecord, RANGE_FORMAT_VERSION, StoredBlock, TreeSizes,
    decode_range_record,
};
use crate::ingest::{EncodedBatch, WriteBatch};

mod metadata;
pub mod range_meta;
mod tables;
mod telemetry;
mod writer;
use metadata::Metadata;
use range_meta::{RangeMeta, VerifyState};
use tables::{SealedRange, Tables, range_start};
use telemetry::{WriteStage, record_state_metrics};
use writer::IndexWriter;

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

    /// Reads the metadata of the sealed range that starts at `start`, if any was written.
    pub fn range_meta(&self, start: u32) -> Result<Option<RangeMeta>, IndexError> {
        let txn = self.env.read_txn()?;
        self.tables.range_meta.get(&txn, start)
    }

    /// Writes the metadata of the sealed range that starts at `start`, replacing any earlier
    /// row. The range must be sealed, and `meta.entry` must describe exactly that range.
    ///
    /// The write does not advance the generation: range metadata is not part of a generation's
    /// block coverage, so readers of the current generation are not disturbed.
    pub fn put_range_meta(&self, start: u32, meta: &RangeMeta) -> Result<(), IndexError> {
        let end = start.checked_add(RANGE_SIZE - 1).ok_or(IndexError::Overflow)?;
        let mut txn = self.env.write_txn()?;
        let state = self.tables.metadata.get_state(&txn)?;
        if start != range_start(start)
            || meta.entry.start_height != start
            || meta.entry.end_height != end
            || state.sealed_through.is_none_or(|sealed| end > sealed)
        {
            return Err(IndexError::RangeMeta { start });
        }
        self.tables.range_meta.put(&mut txn, start, meta)?;
        txn.commit()?;
        Ok(())
    }

    /// Reads the packed blob of the sealed range that starts at `start`. The bytes are the same
    /// as in `sealed_ranges`. Returns `None` if no sealed range starts at `start`. Reads the
    /// current generation.
    pub fn read_sealed_range_bytes(&self, start: u32) -> Result<Option<Vec<u8>>, IndexError> {
        let txn = self.env.read_txn()?;
        self.sealed_range_bytes_in(&txn, start)
    }

    /// Reads the packed blob of the sealed range that starts at `start` from a generation-pinned
    /// LMDB snapshot. Returns `None` if no sealed range starts at `start`.
    pub fn read_sealed_range_bytes_at(
        &self,
        generation: u64,
        start: u32,
    ) -> Result<Option<Vec<u8>>, IndexError> {
        let txn = self.env.read_txn()?;
        self.read_generation(&txn, generation)?;
        self.sealed_range_bytes_in(&txn, start)
    }

    fn sealed_range_bytes_in(
        &self,
        txn: &RoTxn<'_>,
        start: u32,
    ) -> Result<Option<Vec<u8>>, IndexError> {
        if start != range_start(start) {
            return Err(IndexError::RangeStart { height: start });
        }
        Ok(self.tables.sealed_ranges.get(txn, start)?.map(<[u8]>::to_vec))
    }

    /// Reads the metadata rows of the sealed ranges that start at or above `start`, in height
    /// order, from a generation-pinned LMDB snapshot. A sealed range with no row is not in the
    /// list.
    pub fn range_metas_from(
        &self,
        generation: u64,
        start: u32,
    ) -> Result<Vec<RangeMeta>, IndexError> {
        let txn = self.env.read_txn()?;
        self.read_generation(&txn, generation)?;
        self.tables.range_meta.rows_from(&txn, start)
    }

    /// Installs one packed range from a snapshot, and its metadata, in one write. The caller
    /// must check the blob digest first.
    ///
    /// Conditions for an install:
    ///
    /// - The index has no mutable rows.
    /// - `start` is `sealed_through + 1`, or 0 on an empty index. Thus ranges install in height
    ///   order, with no gaps.
    /// - Each record of the blob decodes and links to the block below it. The first record links
    ///   to the tip, or to the genesis previous hash for range 0.
    /// - The tree sizes do not decrease from the tip to the last record.
    /// - No block hash of the blob is in the index already, and no hash occurs two times.
    /// - `meta.entry` gives the range format version, the heights, the end hash and the end tree
    ///   sizes of the blob.
    /// - `meta.state` is not [`VerifyState::Rejected`].
    ///
    /// The method does the checks on the state before it decodes the blob.
    ///
    /// The write adds the blob, its hash entries and `meta`. It moves the tip and
    /// `sealed_through` to the range end. A [`VerifyState::Verified`] range advances
    /// `verified_through` if the old value is `start - 1`, or not set for range 0. Other states
    /// do not change `verified_through`.
    pub fn install_sealed_range(
        &self,
        start: u32,
        blob: &[u8],
        meta: &RangeMeta,
    ) -> Result<IndexState, IndexError> {
        let mut writer = IndexWriter::start_latest(&self.env, self.tables)?;
        let state = writer.state();
        let next = state.next_range_start()?;
        if start != next {
            return Err(IndexError::RangeOrder { start, next });
        }
        if state.has_mutable_rows() {
            return Err(IndexError::MutableRows);
        }
        // The index has no mutable rows. Thus the tip is the last block of the sealed ranges.
        let parent = state.durable_tip.map_or(GENESIS_PREVIOUS_BLOCK_HASH, |tip| tip.hash);
        let chain = SealedRange::new(start, blob)?.check_chain(parent, state.tree_sizes)?;
        let last = &chain.last;
        let entry = &meta.entry;
        if entry.range_version != RANGE_FORMAT_VERSION
            || entry.start_height != start
            || entry.end_height != last.height
            || entry.end_hash != last.hash
            || entry.end_tree_sizes != last.end_tree_sizes
            || meta.state == VerifyState::Rejected
        {
            return Err(IndexError::RangeMeta { start });
        }

        writer.put_new_hashes(&chain.ids)?;
        writer.put_sealed_range(start, blob, meta)?;
        writer.set_tip(BlockId::new(last.height, last.hash), last.end_tree_sizes);
        writer.commit()
    }

    pub fn read_block(
        &self,
        generation: u64,
        height: u32,
    ) -> Result<CompactBlockRecord, IndexError> {
        self.read_block_as(generation, height)
    }

    /// Reads one block from a generation-pinned LMDB snapshot.
    pub fn read_block_as<T: StoredBlock>(
        &self,
        generation: u64,
        height: u32,
    ) -> Result<T, IndexError> {
        let txn = self.env.read_txn()?;
        let state = self.read_generation(&txn, generation)?;
        self.tables.read(&txn, state, height)
    }

    pub fn read_block_by_hash(
        &self,
        generation: u64,
        hash: block::Hash,
    ) -> Result<Option<CompactBlockRecord>, IndexError> {
        self.read_block_by_hash_as(generation, hash)
    }

    /// Resolves and reads one canonical block hash from a generation-pinned LMDB snapshot.
    pub fn read_block_by_hash_as<T: StoredBlock>(
        &self,
        generation: u64,
        hash: block::Hash,
    ) -> Result<Option<T>, IndexError> {
        let txn = self.env.read_txn()?;
        let state = self.read_generation(&txn, generation)?;
        self.tables
            .hash_to_height
            .get(&txn, &hash)?
            .map(|height| self.tables.read(&txn, state, height))
            .transpose()
    }

    /// Incrementally reads an inclusive range under one LMDB read transaction.
    ///
    /// Returning `false` from `emit` stops cleanly, allowing a cancelled client to release the
    /// transaction without decoding the rest of the range.
    pub fn read_range(
        &self,
        generation: u64,
        start: u32,
        end: u32,
        emit: impl FnMut(CompactBlockRecord) -> bool,
    ) -> Result<(), IndexError> {
        let txn = self.env.read_txn()?;
        let state = self.read_generation(&txn, generation)?;
        self.read_range_in(&txn, state, start, end, emit)
    }

    pub fn read_range_latest(
        &self,
        start: u32,
        end: u32,
        emit: impl FnMut(CompactBlockRecord) -> bool,
    ) -> Result<(), IndexError> {
        self.read_range_latest_as(start, end, emit)
    }

    /// Reads an inclusive range against the current generation, with no generation check:
    /// a caller streaming across commits verifies chain continuity between calls itself.
    pub fn read_range_latest_as<T: StoredBlock>(
        &self,
        start: u32,
        end: u32,
        emit: impl FnMut(T) -> bool,
    ) -> Result<(), IndexError> {
        let txn = self.env.read_txn()?;
        let state = self.tables.metadata.get_state(&txn)?;
        self.read_range_in(&txn, state, start, end, emit)
    }

    fn read_range_in<T: StoredBlock>(
        &self,
        txn: &RoTxn<'_>,
        state: IndexState,
        start: u32,
        end: u32,
        mut emit: impl FnMut(T) -> bool,
    ) -> Result<(), IndexError> {
        let tip = state.durable_tip.ok_or(IndexError::Coverage { height: start })?;
        if start > tip.height {
            return Err(IndexError::Coverage { height: start });
        }
        if end > tip.height {
            return Err(IndexError::Coverage { height: end });
        }

        let ascending = start <= end;
        let mut height = start;
        loop {
            if state.sealed_through.is_some_and(|sealed| height <= sealed) {
                let range = self
                    .tables
                    .sealed_ranges
                    .find(txn, height)?
                    .ok_or(IndexError::Coverage { height })?;
                let chunk_end =
                    if ascending { end.min(range.end()) } else { end.max(range.start()) };
                loop {
                    if !emit(range.read(height)?) {
                        return Ok(());
                    }
                    if height == chunk_end {
                        break;
                    }
                    height = if ascending { height + 1 } else { height - 1 };
                }
            } else if !emit(self.tables.read(txn, state, height)?) {
                return Ok(());
            }

            if height == end {
                return Ok(());
            }
            height = if ascending { height + 1 } else { height - 1 };
        }
    }

    fn read_generation(&self, txn: &RoTxn<'_>, generation: u64) -> Result<IndexState, IndexError> {
        let state = self.tables.metadata.get_state(txn)?;
        if state.generation != generation {
            return Err(IndexError::Generation { expected: generation, actual: state.generation });
        }
        Ok(state)
    }

    /// Checks sealed-range summaries and every mutable suffix row without rescanning sealed blocks.
    pub fn verify_continuity(&self) -> Result<(), IndexError> {
        let txn = self.env.read_txn()?;
        let Tables { metadata, sealed_ranges, mutable_blocks, hash_to_height, range_meta } =
            self.tables;
        let state = metadata.get_state(&txn)?;
        let Some(tip) = state.durable_tip else {
            if self.tables.has_blocks(&txn)? || !range_meta.starts(&txn)?.is_empty() {
                return Err(IndexError::Metadata);
            }
            return Ok(());
        };
        for start in range_meta.starts(&txn)? {
            if start != range_start(start) || sealed_ranges.get(&txn, start)?.is_none() {
                return Err(IndexError::RangeMeta { start });
            }
        }

        let sealed_blocks = state.sealed_through.map_or(0, |height| u64::from(height) + 1);
        if sealed_ranges.len(&txn)? != sealed_blocks / u64::from(RANGE_SIZE)
            || mutable_blocks.len(&txn)? != u64::from(tip.height) + 1 - sealed_blocks
            || hash_to_height.len(&txn)? != u64::from(tip.height) + 1
        {
            return Err(IndexError::Continuity { height: 0 });
        }

        let mut next_height = 0u64;
        let mut previous_hash = None;
        for entry in sealed_ranges.iter(&txn)? {
            let (start, bytes) = entry?;
            if u64::from(start) != next_height {
                return Err(IndexError::Continuity { height: next_height as u32 });
            }
            let first = decode_range_record(bytes, 0)?;
            let last = decode_range_record(bytes, RANGE_SIZE as usize - 1)?;
            if previous_hash.is_some_and(|hash| first.previous_hash != hash) {
                return Err(IndexError::Continuity { height: start });
            }
            hash_to_height.check(&txn, &first)?;
            hash_to_height.check(&txn, &last)?;
            previous_hash = Some(last.hash);
            next_height += u64::from(RANGE_SIZE);
        }
        if next_height != sealed_blocks {
            return Err(IndexError::Continuity { height: next_height as u32 });
        }

        for entry in mutable_blocks.iter(&txn)? {
            let (height, bytes) = entry?;
            if u64::from(height) != next_height {
                return Err(IndexError::Continuity { height: next_height as u32 });
            }
            let record = CompactBlockRecord::decode(bytes)?;
            if record.height != height
                || previous_hash.is_some_and(|hash| record.previous_hash != hash)
            {
                return Err(IndexError::Continuity { height });
            }
            hash_to_height.check(&txn, &record)?;
            previous_hash = Some(record.hash);
            next_height += 1;
        }
        let missing_tip = IndexError::Continuity { height: tip.height };
        let tip_record: CompactBlockRecord = if state
            .sealed_through
            .is_some_and(|sealed| tip.height <= sealed)
        {
            sealed_ranges.find(&txn, tip.height)?.ok_or(missing_tip)?.read(tip.height)?
        } else {
            CompactBlockRecord::decode(mutable_blocks.get(&txn, tip.height)?.ok_or(missing_tip)?)?
        };
        if next_height != u64::from(tip.height) + 1
            || previous_hash != Some(tip.hash)
            || state.tree_sizes != tip_record.end_tree_sizes
        {
            return Err(IndexError::Continuity { height: tip.height });
        }
        Ok(())
    }

    /// Atomically writes a batch and packs newly sealed ranges.
    pub fn write(&self, batch: WriteBatch) -> Result<IndexState, IndexError> {
        let started = Instant::now();
        let batch = batch.encode(1)?;
        let encode = started.elapsed();
        let state = self.write_encoded(batch)?;
        WriteStage::Encode.record(encode);
        Ok(state)
    }

    /// Atomically writes a batch with encoded records and packs newly sealed ranges.
    pub(crate) fn write_encoded(&self, batch: EncodedBatch) -> Result<IndexState, IndexError> {
        let EncodedBatch { base_generation, seal_through, blocks, end_tree_sizes } = batch;
        let mut writer = IndexWriter::start(&self.env, self.tables, base_generation)?;
        let state = writer.state();
        let first = blocks.first().expect("builders never emit empty batches");
        let expected_height = match state.durable_tip {
            Some(tip) => tip.height.checked_add(1).ok_or(IndexError::Overflow)?,
            None => 0,
        };
        if first.height != expected_height
            || state.durable_tip.is_some_and(|tip| first.previous_hash != tip.hash)
        {
            return Err(IndexError::StaleBatch { batch_generation: base_generation });
        }

        writer.start_timer();
        writer.put_hashes(&blocks)?;
        let last = blocks.last().expect("the batch is non-empty");
        writer.set_tip(BlockId::new(last.height, last.hash), end_tree_sizes);
        let mut blocks = blocks.into_iter().peekable();
        writer.seal_ranges(seal_through, &mut blocks)?;
        writer.put_mutable_blocks(blocks)?;
        writer.commit()
    }

    /// Atomically replaces every mutable block after `ancestor`.
    pub fn replace_mutable_suffix(
        &self,
        base_generation: u64,
        ancestor: BlockId,
        records: Vec<CompactBlockRecord>,
        seal_through: Option<u32>,
    ) -> Result<IndexState, IndexError> {
        let mut writer = IndexWriter::start(&self.env, self.tables, base_generation)?;
        let state = writer.state();
        if state.sealed_through.is_some_and(|sealed| ancestor.height < sealed) {
            return Err(IndexError::Sealed { height: ancestor.height + 1 });
        }
        let ancestor_record = writer.read(ancestor.height)?;
        if ancestor_record.hash != ancestor.hash {
            return Err(IndexError::Replacement { height: ancestor.height });
        }

        let delete_start = ancestor.height.checked_add(1).ok_or(IndexError::Overflow)?;
        let mut expected_height = delete_start;
        let mut previous_hash = ancestor.hash;
        for record in &records {
            if record.height != expected_height || record.previous_hash != previous_hash {
                return Err(IndexError::Replacement { height: expected_height });
            }
            expected_height = expected_height.checked_add(1).ok_or(IndexError::Overflow)?;
            previous_hash = record.hash;
        }

        let old_tip = state.durable_tip.ok_or(IndexError::Metadata)?;
        if ancestor.height > old_tip.height {
            return Err(IndexError::Replacement { height: ancestor.height });
        }
        (delete_start..=old_tip.height)
            .try_for_each(|height| writer.delete_mutable_block(height))?;
        records.iter().try_for_each(|record| writer.put_block(record))?;

        let (tip, tree_sizes) =
            records.last().map_or((ancestor, ancestor_record.end_tree_sizes), |record| {
                (BlockId::new(record.height, record.hash), record.end_tree_sizes)
            });
        writer.set_tip(tip, tree_sizes);
        writer.seal_ranges(seal_through, &mut Vec::new().into_iter().peekable())?;
        writer.commit()
    }

    /// Atomically rebuilds every physical range touched by a deep reorg.
    pub fn replace_deep_suffix(
        &self,
        base_generation: u64,
        common_ancestor: BlockId,
        replacement: Vec<CompactBlockRecord>,
        seal_through: Option<u32>,
    ) -> Result<IndexState, IndexError> {
        let mut writer = IndexWriter::start(&self.env, self.tables, base_generation)?;
        let old_tip = writer.state().durable_tip.ok_or(IndexError::Metadata)?;
        let common = writer.read(common_ancestor.height)?;
        if common.hash != common_ancestor.hash {
            return Err(IndexError::Replacement { height: common_ancestor.height });
        }

        let affected_start = range_start(common_ancestor.height);
        let old_records = (affected_start..=old_tip.height)
            .map(|height| writer.read(height))
            .collect::<Result<Vec<_>, _>>()?;
        let prefix_len = (common_ancestor.height - affected_start + 1) as usize;
        let mut canonical = old_records[..prefix_len].to_vec();
        canonical.extend(replacement);
        if canonical.windows(2).any(|pair| {
            pair[0].height.checked_add(1) != Some(pair[1].height)
                || pair[1].previous_hash != pair[0].hash
        }) {
            return Err(IndexError::Replacement { height: common_ancestor.height });
        }

        // Do all deletes before the first put: `canonical` shares its prefix hashes
        // with `old_records`.
        old_records.iter().try_for_each(|record| writer.delete_block(record))?;
        writer.delete_ranges_from(affected_start)?;
        canonical.iter().try_for_each(|record| writer.put_block(record))?;
        let tip = canonical.last().expect("the common-ancestor prefix is non-empty");
        writer.set_tip(BlockId::new(tip.height, tip.hash), tip.end_tree_sizes);
        writer.seal_ranges(seal_through, &mut Vec::new().into_iter().peekable())?;
        writer.commit()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{hash, prepared, record};
    use crate::{
        codec::{ProtobufBlockRecord, decode_range_record, encode_range},
        index::range_meta::full_meta,
        ingest::OrderedBuilder,
    };

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

    /// The pinned reads refuse another generation. The row list starts at the given height.
    #[test]
    fn pinned_range_reads_check_the_generation() {
        let dir = tempfile::tempdir().unwrap();
        let index =
            Index::open(dir.path(), 20 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap();
        let generation = index.write(batch(&index, 0..=2_100, 2_200)).unwrap().generation();
        index.put_range_meta(0, &full_meta(0)).unwrap();
        index.put_range_meta(1_000, &full_meta(1_000)).unwrap();

        assert_eq!(
            index.range_metas_from(generation, 0).unwrap(),
            [full_meta(0), full_meta(1_000)]
        );
        assert_eq!(index.range_metas_from(generation, 1).unwrap(), [full_meta(1_000)]);
        assert_eq!(
            index.read_sealed_range_bytes_at(generation, 1_000).unwrap(),
            index.read_sealed_range_bytes(1_000).unwrap()
        );
        assert_eq!(index.read_sealed_range_bytes_at(generation, 2_000).unwrap(), None);
        let other = generation + 1;
        assert!(matches!(
            index.range_metas_from(other, 0),
            Err(IndexError::Generation { expected, actual }) if expected == other && actual == generation
        ));
        assert!(matches!(
            index.read_sealed_range_bytes_at(other, 0),
            Err(IndexError::Generation { .. })
        ));
    }

    #[test]
    fn range_meta_round_trips_for_a_sealed_range_and_refuses_the_rest() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index");
        let index = Index::open(&path, 20 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap();
        let state = index.write(batch(&index, 0..=2_100, 2_200)).unwrap();
        assert_eq!(state.sealed_through(), Some(1_999));

        index.put_range_meta(0, &full_meta(0)).unwrap();
        index.put_range_meta(1_000, &full_meta(1_000)).unwrap();
        assert_eq!(index.range_meta(0).unwrap(), Some(full_meta(0)));
        assert_eq!(index.range_meta(1_000).unwrap(), Some(full_meta(1_000)));
        assert_eq!(index.range_meta(2_000).unwrap(), None);
        // Metadata writes leave the generation alone.
        assert_eq!(index.state().unwrap(), state);

        // The range at 2,000 is not sealed; 500 is not a range start; the entry must match.
        let refused = |start| matches!(index.put_range_meta(start, &full_meta(start)), Err(IndexError::RangeMeta { start: s }) if s == start);
        assert!(refused(2_000));
        assert!(refused(500));
        assert!(matches!(
            index.put_range_meta(0, &full_meta(1_000)),
            Err(IndexError::RangeMeta { start: 0 })
        ));
        assert_eq!(index.range_meta(0).unwrap(), Some(full_meta(0)));

        drop(index);
        let reopened =
            Index::open(&path, 20 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap();
        assert_eq!(reopened.range_meta(1_000).unwrap(), Some(full_meta(1_000)));
        assert_eq!(reopened.state().unwrap(), state);
    }

    #[test]
    fn continuity_rejects_metadata_for_a_range_that_is_not_sealed() {
        let dir = tempfile::tempdir().unwrap();
        let index =
            Index::open(dir.path(), 20 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap();
        index.write(batch(&index, 0..=1_100, 1_200)).unwrap();

        let mut txn = index.env.write_txn().unwrap();
        index.tables.range_meta.put(&mut txn, 1_000, &full_meta(1_000)).unwrap();
        txn.commit().unwrap();
        assert!(matches!(index.verify_continuity(), Err(IndexError::RangeMeta { start: 1_000 })));

        let mut txn = index.env.write_txn().unwrap();
        index.tables.range_meta.delete_from(&mut txn, 1_000).unwrap();
        txn.commit().unwrap();
        index.verify_continuity().unwrap();
    }

    #[test]
    fn deep_reorg_drops_the_metadata_and_the_verified_prefix_of_rebuilt_ranges() {
        let dir = tempfile::tempdir().unwrap();
        let index =
            Index::open(dir.path(), 20 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap();
        let old = index.write(batch(&index, 0..=2_100, 2_200)).unwrap();
        assert_eq!(old.sealed_through(), Some(1_999));
        index.put_range_meta(0, &full_meta(0)).unwrap();
        index.put_range_meta(1_000, &full_meta(1_000)).unwrap();
        let old = IndexState { verified_through: Some(1_999), ..old };
        let mut txn = index.env.write_txn().unwrap();
        index.tables.metadata.put_state(&mut txn, old).unwrap();
        txn.commit().unwrap();
        assert_eq!(index.state().unwrap().verified_through(), Some(1_999));

        // The reorg rebuilds the range 1,000..=1,999: its metadata goes, the first range's stays,
        // and the verified prefix ends with the first range.
        let common = BlockId::new(1_950, hash(1_950));
        let replacement = branch(common, 1_951..=2_110);
        let state =
            index.replace_deep_suffix(old.generation(), common, replacement, Some(2_010)).unwrap();

        assert_eq!(state.sealed_through(), Some(1_999));
        assert_eq!(state.verified_through(), Some(999));
        assert_eq!(index.range_meta(0).unwrap(), Some(full_meta(0)));
        assert_eq!(index.range_meta(1_000).unwrap(), None);
        index.verify_continuity().unwrap();

        // A reorg into the first range ends the verified prefix altogether.
        let common = BlockId::new(950, hash(950));
        let replacement = branch(common, 951..=2_110);
        let state = index
            .replace_deep_suffix(state.generation(), common, replacement, Some(2_010))
            .unwrap();
        assert_eq!(state.verified_through(), None);
        assert_eq!(index.range_meta(0).unwrap(), None);
        index.verify_continuity().unwrap();
    }

    fn tree_sizes(height: u32) -> TreeSizes {
        TreeSizes { sapling: height, orchard: height / 2, ironwood: 0 }
    }

    /// The records of the range at `start`. The tree sizes increase with the height.
    fn range_records(start: u32) -> Vec<CompactBlockRecord> {
        (start..start + RANGE_SIZE)
            .map(|height| CompactBlockRecord {
                end_tree_sizes: tree_sizes(height),
                ..record(height, Vec::new())
            })
            .collect()
    }

    /// The blob of the range at `start`, as a snapshot gives it, and its metadata with `state`.
    fn snapshot_range(start: u32, state: VerifyState) -> (Vec<u8>, RangeMeta) {
        let mut meta = full_meta(start);
        meta.entry.end_tree_sizes = tree_sizes(start + RANGE_SIZE - 1);
        meta.state = state;
        (encode_range(&range_records(start)).unwrap(), meta)
    }

    fn install(index: &Index, start: u32, state: VerifyState) -> Result<IndexState, IndexError> {
        let (blob, meta) = snapshot_range(start, state);
        index.install_sealed_range(start, &blob, &meta)
    }

    #[test]
    fn installs_sealed_ranges_in_order_and_reads_them_back() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index");
        let index = Index::open(&path, 20 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap();
        for start in [0, 1_000, 2_000] {
            install(&index, start, VerifyState::Verified).unwrap();
        }
        let state = index.state().unwrap();
        assert_eq!(state.durable_tip(), Some(BlockId::new(2_999, hash(2_999))));
        assert_eq!(state.sealed_through(), Some(2_999));
        assert_eq!(state.verified_through(), Some(2_999));
        assert_eq!(state.tree_sizes(), tree_sizes(2_999));
        assert_eq!(state.generation(), 3);
        index.verify_continuity().unwrap();

        let (blob, meta) = snapshot_range(1_000, VerifyState::Verified);
        assert_eq!(index.read_sealed_range_bytes(1_000).unwrap(), Some(blob));
        assert_eq!(index.read_sealed_range_bytes(3_000).unwrap(), None);
        assert!(matches!(
            index.read_sealed_range_bytes(1_500),
            Err(IndexError::RangeStart { height: 1_500 })
        ));
        assert_eq!(index.range_meta(1_000).unwrap(), Some(meta));
        for height in [0, 999, 1_000, 1_537, 2_999] {
            let block = index.read_block(state.generation(), height).unwrap();
            assert_eq!((block.hash, block.end_tree_sizes), (hash(height), tree_sizes(height)));
            let block = index.read_block_by_hash(state.generation(), hash(height)).unwrap();
            assert_eq!(block.unwrap().height, height);
        }

        drop(index);
        let reopened =
            Index::open(&path, 20 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap();
        assert_eq!(reopened.state().unwrap(), state);
        assert_eq!(
            reopened.range_meta(2_000).unwrap(),
            Some(snapshot_range(2_000, VerifyState::Verified).1)
        );
    }

    #[test]
    fn install_refuses_a_range_out_of_order() {
        let dir = tempfile::tempdir().unwrap();
        let index =
            Index::open(dir.path(), 20 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap();
        assert!(matches!(
            install(&index, 1_000, VerifyState::Verified),
            Err(IndexError::RangeOrder { start: 1_000, next: 0 })
        ));
        let state = install(&index, 0, VerifyState::Verified).unwrap();
        assert!(matches!(
            install(&index, 2_000, VerifyState::Verified),
            Err(IndexError::RangeOrder { start: 2_000, next: 1_000 })
        ));
        assert!(matches!(
            install(&index, 0, VerifyState::Verified),
            Err(IndexError::RangeOrder { start: 0, next: 1_000 })
        ));
        // The order check occurs before the decode. Thus an empty blob gets the same error.
        let meta = snapshot_range(2_000, VerifyState::Verified).1;
        assert!(matches!(
            index.install_sealed_range(2_000, &[], &meta),
            Err(IndexError::RangeOrder { start: 2_000, next: 1_000 })
        ));
        assert_eq!(index.state().unwrap(), state);
        assert_eq!(index.read_sealed_range_bytes(2_000).unwrap(), None);
    }

    #[test]
    fn install_refuses_a_blob_that_does_not_continue_the_chain() {
        let dir = tempfile::tempdir().unwrap();
        let index =
            Index::open(dir.path(), 20 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap();
        let changed = |start, change: fn(&mut [CompactBlockRecord])| {
            let mut records = range_records(start);
            change(&mut records);
            encode_range(&records).unwrap()
        };
        let refused_at = |start, blob: &[u8], height| {
            let meta = snapshot_range(start, VerifyState::Verified).1;
            matches!(index.install_sealed_range(start, blob, &meta), Err(IndexError::RangeBlob { height: h }) if h == height)
        };
        let unlinked = |records: &mut [CompactBlockRecord]| {
            records[0].previous_hash = block::Hash([9; 32]);
        };
        // Range 0 must link to the genesis previous hash. A blob must hold the range at `start`.
        assert!(refused_at(0, &changed(0, unlinked), 0));
        assert!(refused_at(0, &snapshot_range(1_000, VerifyState::Verified).0, 0));
        install(&index, 0, VerifyState::Verified).unwrap();
        assert!(refused_at(1_000, &changed(1_000, unlinked), 1_000));

        // The tree sizes must not decrease from the tip, or inside the range.
        let smaller_than_tip = |records: &mut [CompactBlockRecord]| {
            records[0].end_tree_sizes = TreeSizes::default();
        };
        assert!(refused_at(1_000, &changed(1_000, smaller_than_tip), 1_000));
        let smaller_inside = |records: &mut [CompactBlockRecord]| {
            records[500].end_tree_sizes.orchard = 0;
        };
        assert!(refused_at(1_000, &changed(1_000, smaller_inside), 1_500));

        // A record in the blob does not link to the record below it. The encoder refuses this
        // range. Thus change the bytes of one record after the encode. The bytes keep their length.
        let (mut blob, meta) = snapshot_range(1_000, VerifyState::Verified);
        let mut bad = range_records(1_000)[500].clone();
        let good = bad.encode().unwrap();
        bad.previous_hash = block::Hash([9; 32]);
        let at = blob.windows(good.len()).position(|bytes| bytes == good).unwrap();
        blob[at..at + good.len()].copy_from_slice(&bad.encode().unwrap());
        assert!(matches!(
            index.install_sealed_range(1_000, &blob, &meta),
            Err(IndexError::RangeBlob { height: 1_500 })
        ));
        assert_eq!(index.state().unwrap().sealed_through(), Some(999));
    }

    #[test]
    fn install_refuses_a_block_hash_that_is_in_the_index_already() {
        let dir = tempfile::tempdir().unwrap();
        let index =
            Index::open(dir.path(), 20 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap();
        let state = install(&index, 0, VerifyState::Verified).unwrap();
        let meta = snapshot_range(1_000, VerifyState::Verified).1;
        // The block at 1,500 gets the hash `duplicate`. The block at 1,501 links to it.
        let blob = |duplicate| {
            let mut records = range_records(1_000);
            records[500].hash = duplicate;
            records[501].previous_hash = duplicate;
            encode_range(&records).unwrap()
        };

        // The hash of block 5 is in range 0.
        assert!(matches!(
            index.install_sealed_range(1_000, &blob(hash(5)), &meta),
            Err(IndexError::DuplicateHash { height: 1_500 })
        ));
        // The hash of block 1,600 occurs at 1,500 and at 1,600 in the same blob.
        assert!(matches!(
            index.install_sealed_range(1_000, &blob(hash(1_600)), &meta),
            Err(IndexError::DuplicateHash { height: 1_600 })
        ));
        assert_eq!(index.state().unwrap(), state);
        assert_eq!(
            index.read_block_by_hash(state.generation(), hash(5)).unwrap().unwrap().height,
            5
        );
        index.verify_continuity().unwrap();
    }

    #[test]
    fn install_refuses_a_meta_that_does_not_describe_the_blob() {
        let dir = tempfile::tempdir().unwrap();
        let index =
            Index::open(dir.path(), 20 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap();
        let (blob, meta) = snapshot_range(0, VerifyState::Verified);
        let changes: [fn(&mut RangeMeta); 6] = [
            |meta| meta.entry.range_version = 2,
            |meta| meta.entry.start_height = 1_000,
            |meta| meta.entry.end_height = 1_000,
            |meta| meta.entry.end_hash = hash(1_000),
            |meta| meta.entry.end_tree_sizes.orchard += 1,
            |meta| meta.state = VerifyState::Rejected,
        ];
        for change in changes {
            let mut meta = meta.clone();
            change(&mut meta);
            assert!(matches!(
                index.install_sealed_range(0, &blob, &meta),
                Err(IndexError::RangeMeta { start: 0 })
            ));
        }
        assert_eq!(index.state().unwrap(), IndexState::default());
        assert_eq!(index.range_meta(0).unwrap(), None);
        index.install_sealed_range(0, &blob, &meta).unwrap();
    }

    #[test]
    fn install_refuses_an_index_with_mutable_rows() {
        let dir = tempfile::tempdir().unwrap();
        let index =
            Index::open(dir.path(), 20 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap();
        let state = index.write(batch(&index, 0..=1_100, 1_200)).unwrap();
        assert_eq!(state.sealed_through(), Some(999));
        assert!(matches!(
            install(&index, 1_000, VerifyState::Verified),
            Err(IndexError::MutableRows)
        ));
        assert_eq!(index.state().unwrap(), state);

        // An index with mutable rows and no sealed range.
        let dir = tempfile::tempdir().unwrap();
        let index =
            Index::open(dir.path(), 20 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap();
        index.write(batch(&index, 0..=10, 20)).unwrap();
        assert!(matches!(install(&index, 0, VerifyState::Verified), Err(IndexError::MutableRows)));
    }

    #[test]
    fn only_a_verified_range_after_the_verified_prefix_advances_verified_through() {
        let dir = tempfile::tempdir().unwrap();
        let index =
            Index::open(dir.path(), 20 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap();
        let verified = |state: IndexState| state.verified_through();
        assert_eq!(verified(install(&index, 0, VerifyState::Verified).unwrap()), Some(999));
        assert_eq!(verified(install(&index, 1_000, VerifyState::ChainOk).unwrap()), Some(999));
        // The range at 1,000 is not verified. Thus the verified prefix stays at 999.
        let state = install(&index, 2_000, VerifyState::Verified).unwrap();
        assert_eq!((state.sealed_through(), verified(state)), (Some(2_999), Some(999)));

        let dir = tempfile::tempdir().unwrap();
        let index =
            Index::open(dir.path(), 20 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap();
        assert_eq!(verified(install(&index, 0, VerifyState::Unverified).unwrap()), None);
        index.verify_continuity().unwrap();
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

    #[test]
    fn atomically_writes_restarts_and_packs_sealed_ranges() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index");
        let index = Index::open(&path, 10 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap();
        let stale = batch(&index, 0..=0, 1_100);
        let state = index.write(batch(&index, 0..=1_000, 1_100)).unwrap();

        assert_eq!(state.durable_tip().unwrap().height, 1_000);
        assert_eq!(state.sealed_through(), Some(999));
        assert_eq!(state.generation(), 1);
        let txn = index.env.read_txn().unwrap();
        assert!(index.tables.mutable_blocks.get(&txn, 0).unwrap().is_none());
        assert!(index.tables.mutable_blocks.get(&txn, 1_000).unwrap().is_some());
        let range = index.tables.sealed_ranges.get(&txn, 0).unwrap().unwrap();
        assert_eq!(decode_range_record(range, 537).unwrap().height, 537);
        assert_eq!(index.tables.hash_to_height.get(&txn, &hash(537)).unwrap(), Some(537));
        drop(txn);

        assert!(matches!(index.write(stale), Err(IndexError::StaleBatch { batch_generation: 0 })));
        assert_eq!(index.state().unwrap(), state);

        drop(index);
        let reopened =
            Index::open(&path, 10 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap();
        assert_eq!(reopened.state().unwrap(), state);
    }

    #[test]
    fn seals_a_range_that_is_complete_in_mutable_rows() {
        let dir = tempfile::tempdir().unwrap();
        let index =
            Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap();
        // The tip is 1,060: blocks to 1,050 are durable, but only blocks to 960 can be sealed.
        // Range 0..=999 stays complete in `mutable_blocks`.
        let state = index.write(batch(&index, 0..=1_050, 1_060)).unwrap();
        assert_eq!(state.sealed_through(), None);

        // The next batch starts after the range, and the range can now be sealed.
        let state = index.write(batch(&index, 1_051..=1_100, 1_110)).unwrap();
        assert_eq!(state.sealed_through(), Some(999));
        assert_eq!(state.durable_tip().unwrap().height, 1_100);
        for height in [0, 537, 999, 1_000, 1_100] {
            assert_eq!(index.read_block(state.generation(), height).unwrap().height, height);
        }
        let txn = index.env.read_txn().unwrap();
        assert!(index.tables.mutable_blocks.get(&txn, 999).unwrap().is_none());
        drop(txn);
        index.verify_continuity().unwrap();
    }

    /// The write stages that one write records, each with one sample.
    fn write_stages(index: &Index, batch: WriteBatch) -> Vec<String> {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            index.write(batch).unwrap();
        });
        let mut stages: Vec<_> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(key, _, _, _)| key.key().name() == "ztreamer.index.write.seconds")
            .map(|(key, _, _, value)| {
                assert!(matches!(value, DebugValue::Histogram(samples) if samples.len() == 1));
                key.key().labels().next().unwrap().value().to_owned()
            })
            .collect();
        stages.sort();
        stages
    }

    #[test]
    fn write_timings_record_only_stages_that_ran() {
        let dir = tempfile::tempdir().unwrap();
        let index =
            Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap();
        // This write seals no range.
        let stages = write_stages(&index, batch(&index, 0..=10, 20));
        assert!(stages.iter().all(|stage| !stage.starts_with("seal_")));
        assert!(stages.contains(&"mutable_put".to_owned()));
        assert!(stages.contains(&"state_put".to_owned()));

        // This write seals a range from its batch only. It reads and writes no mutable rows.
        let dir = tempfile::tempdir().unwrap();
        let index =
            Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap();
        let stages = write_stages(&index, batch(&index, 0..=999, 1_100));
        assert!(stages.contains(&"seal_encode".to_owned()));
        assert!(stages.contains(&"seal_put".to_owned()));
        assert!(!stages.contains(&"seal_read".to_owned()));
        assert!(!stages.contains(&"mutable_put".to_owned()));

        // This write seals a range from mutable rows and batch rows, then writes the rest of the
        // batch as mutable rows.
        let dir = tempfile::tempdir().unwrap();
        let index =
            Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap();
        index.write(batch(&index, 0..=500, 600)).unwrap();
        let stages = write_stages(&index, batch(&index, 501..=1_050, 1_110));
        for stage in ["seal_read", "seal_encode", "seal_put", "mutable_put"] {
            assert!(stages.contains(&stage.to_owned()), "{stage} is missing from {stages:?}");
        }
    }

    #[test]
    fn seal_fails_for_a_damaged_mutable_row() {
        let dir = tempfile::tempdir().unwrap();
        let index =
            Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap();
        index.write(batch(&index, 0..=10, 20)).unwrap();

        // Change the hash in the payload. The hash in the envelope stays correct.
        let mut txn = index.env.write_txn().unwrap();
        let mut row = index.tables.mutable_blocks.get(&txn, 5).unwrap().unwrap().to_vec();
        let payload_hash = row.windows(32).rposition(|w| w == hash(5).0).unwrap();
        row[payload_hash] ^= 1;
        index.tables.mutable_blocks.put(&mut txn, 5, &row).unwrap();
        txn.commit().unwrap();

        assert!(matches!(
            index.write(batch(&index, 11..=1_000, 1_100)),
            Err(IndexError::Codec(CodecError::InvalidRecord))
        ));
        assert_eq!(index.state().unwrap().sealed_through(), None);
    }

    #[test]
    fn seal_fails_for_a_mutable_row_without_shielded_data() {
        use prost::Message;

        let dir = tempfile::tempdir().unwrap();
        let index =
            Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap();
        index.write(batch(&index, 0..=10, 20)).unwrap();

        // Add a transaction without shielded data to a row. The row still decodes, but
        // `CompactBlockRecord::encode` does not accept it, so the seal must not accept it.
        let mut txn = index.env.write_txn().unwrap();
        let row = index.tables.mutable_blocks.get(&txn, 5).unwrap().unwrap().to_vec();
        let mut block = ProtobufBlockRecord::decode(&row).unwrap().0;
        block
            .vtx
            .push(ztreamer_protocol::proto::CompactTx { txid: vec![7; 32], ..Default::default() });
        let payload = block.encode_to_vec();
        let envelope = 1 + size_of::<u32>() + 2 * size_of::<block::Hash>();
        let mut damaged = row[..envelope].to_vec();
        damaged.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        damaged.extend_from_slice(&payload);
        CompactBlockRecord::decode(&damaged).unwrap();
        index.tables.mutable_blocks.put(&mut txn, 5, &damaged).unwrap();
        txn.commit().unwrap();

        assert!(matches!(
            index.write(batch(&index, 11..=1_000, 1_100)),
            Err(IndexError::Codec(CodecError::InvalidRecord))
        ));
        assert_eq!(index.state().unwrap().sealed_through(), None);
    }

    #[test]
    fn restart_rejects_a_gap_in_mutable_coverage() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index");
        let index = Index::open(&path, 10 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap();
        index.write(batch(&index, 0..=10, 20)).unwrap();
        let mut txn = index.env.write_txn().unwrap();
        index.tables.mutable_blocks.delete(&mut txn, 5).unwrap();
        txn.commit().unwrap();
        drop(index);

        assert!(matches!(
            Index::open(&path, 10 * 1024 * 1024, "Mainnet", block::Hash([1; 32])),
            Err(IndexError::Continuity { .. })
        ));
    }

    #[test]
    fn persistence_and_sealing_use_exact_depth_boundaries() {
        let dir = tempfile::tempdir().unwrap();
        let index =
            Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap();
        let mut builder = OrderedBuilder::new(IndexState::default(), 10 * 1024 * 1024).unwrap();
        builder.push(prepared(0)).unwrap();

        assert!(builder.build_batch(None, None, 10 * 1024 * 1024).unwrap().is_none());
        let txn = index.env.read_txn().unwrap();
        assert!(index.tables.mutable_blocks.get(&txn, 0).unwrap().is_none());
        drop(txn);

        let state = index
            .write(builder.build_batch(Some(0), None, 10 * 1024 * 1024).unwrap().unwrap())
            .unwrap();
        let txn = index.env.read_txn().unwrap();
        assert!(index.tables.mutable_blocks.get(&txn, 0).unwrap().is_some());
        drop(txn);

        let mut builder = OrderedBuilder::new(state, 10 * 1024 * 1024).unwrap();
        for height in 1..=90 {
            builder.push(prepared(height)).unwrap();
        }
        let state = index
            .write(builder.build_batch(Some(90), Some(0), 10 * 1024 * 1024).unwrap().unwrap())
            .unwrap();
        let txn = index.env.read_txn().unwrap();
        assert!(index.tables.mutable_blocks.get(&txn, 0).unwrap().is_some());
        assert!(index.tables.sealed_ranges.get(&txn, 0).unwrap().is_none());
        drop(txn);

        let mut builder = OrderedBuilder::new(state, 10 * 1024 * 1024).unwrap();
        for height in 91..RANGE_SIZE {
            builder.push(prepared(height)).unwrap();
        }
        let state = index
            .write(builder.build_batch(Some(1_089), Some(999), 10 * 1024 * 1024).unwrap().unwrap())
            .unwrap();
        let txn = index.env.read_txn().unwrap();
        assert_eq!(state.sealed_through(), Some(999));
        assert!(index.tables.mutable_blocks.get(&txn, 0).unwrap().is_none());
        assert!(index.tables.sealed_ranges.get(&txn, 0).unwrap().is_some());
    }

    #[test]
    fn aborted_range_pack_keeps_every_mutable_row() {
        let dir = tempfile::tempdir().unwrap();
        let index =
            Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap();
        let state = index.write(batch(&index, 0..=999, 1_009)).unwrap();
        assert_eq!(state.durable_tip().unwrap().height, 999);

        let mut txn = index.env.write_txn().unwrap();
        let records = (0..RANGE_SIZE)
            .map(|height| {
                CompactBlockRecord::decode(
                    index.tables.mutable_blocks.get(&txn, height).unwrap().unwrap(),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let encoded = crate::codec::encode_range(&records).unwrap();
        index.tables.sealed_ranges.put(&mut txn, 0, encoded.as_slice()).unwrap();
        for height in 0..RANGE_SIZE / 2 {
            index.tables.mutable_blocks.delete(&mut txn, height).unwrap();
        }
        drop(txn); // simulated failure before commit

        let txn = index.env.read_txn().unwrap();
        assert!(index.tables.sealed_ranges.get(&txn, 0).unwrap().is_none());
        assert_eq!(index.tables.mutable_blocks.len(&txn).unwrap(), 1_000);
        assert_eq!(index.tables.hash_to_height.len(&txn).unwrap(), 1_000);
        drop(txn);
        assert_eq!(index.state().unwrap(), state);
        index.verify_continuity().unwrap();
    }

    #[test]
    fn reads_sealed_and_mutable_ranges_in_both_directions() {
        let dir = tempfile::tempdir().unwrap();
        let index =
            Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap();
        let state = index.write(batch(&index, 0..=1_005, 1_100)).unwrap();

        assert_eq!(index.read_block(state.generation(), 537).unwrap().height, 537);
        assert_eq!(
            index.read_block_by_hash(state.generation(), hash(1_002)).unwrap().unwrap().height,
            1_002
        );

        let mut ascending = Vec::new();
        index
            .read_range(state.generation(), 998, 1_002, |block| {
                ascending.push(block.height);
                true
            })
            .unwrap();
        assert_eq!(ascending, [998, 999, 1_000, 1_001, 1_002]);

        let mut descending = Vec::new();
        index
            .read_range(state.generation(), 1_002, 998, |block| {
                descending.push(block.height);
                true
            })
            .unwrap();
        assert_eq!(descending, [1_002, 1_001, 1_000, 999, 998]);
        assert!(matches!(
            index.read_block(state.generation() - 1, 0),
            Err(IndexError::Generation { .. })
        ));

        let mut latest = Vec::new();
        index
            .read_range_latest(1_002, 998, |block| {
                latest.push(block.height);
                true
            })
            .unwrap();
        assert_eq!(latest, descending);
    }

    #[test]
    fn atomically_replaces_a_mutable_suffix() {
        let dir = tempfile::tempdir().unwrap();
        let index =
            Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap();
        let old = index.write(batch(&index, 0..=50, 60)).unwrap();
        let ancestor = BlockId::new(30, hash(30));
        let replacement = branch(ancestor, 31..=55);
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        let (state, pinned_hashes) = std::thread::scope(|scope| {
            let reader_index = &index;
            let reader = scope.spawn(move || {
                let mut pinned_hashes = Vec::new();
                reader_index
                    .read_range(old.generation(), 29, 32, |block| {
                        if block.height == 29 {
                            started_tx.send(()).unwrap();
                            release_rx.recv().unwrap();
                        }
                        pinned_hashes.push(block.hash);
                        true
                    })
                    .unwrap();
                pinned_hashes
            });
            started_rx.recv().unwrap();
            let state = index
                .replace_mutable_suffix(old.generation(), ancestor, replacement, None)
                .unwrap();
            release_tx.send(()).unwrap();
            (state, reader.join().unwrap())
        });

        assert_eq!(state.durable_tip(), Some(BlockId::new(55, branch_hash(55))));
        assert_eq!(pinned_hashes, [hash(29), hash(30), hash(31), hash(32)]);
        assert!(index.read_block_by_hash(state.generation(), hash(31)).unwrap().is_none());
        assert_eq!(index.read_block(state.generation(), 31).unwrap().hash, branch_hash(31));
        index.verify_continuity().unwrap();
    }

    #[test]
    fn mutable_reorg_seals_a_range_that_it_completes() {
        let dir = tempfile::tempdir().unwrap();
        let index =
            Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap();
        let old = index.write(batch(&index, 0..=1_050, 1_060)).unwrap();
        assert_eq!(old.sealed_through(), None);

        let ancestor = BlockId::new(1_040, hash(1_040));
        let replacement = branch(ancestor, 1_041..=1_045);
        let state = index
            .replace_mutable_suffix(old.generation(), ancestor, replacement, Some(999))
            .unwrap();

        assert_eq!(state.sealed_through(), Some(999));
        assert_eq!(state.durable_tip(), Some(BlockId::new(1_045, branch_hash(1_045))));
        let txn = index.env.read_txn().unwrap();
        assert_eq!(index.tables.mutable_blocks.len(&txn).unwrap(), 46);
        drop(txn);
        assert_eq!(index.read_block(state.generation(), 537).unwrap().hash, hash(537));
        index.verify_continuity().unwrap();
    }

    #[test]
    fn deep_reorg_rebuilds_the_affected_sealed_range() {
        let dir = tempfile::tempdir().unwrap();
        let index =
            Index::open(dir.path(), 20 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap();
        let old = index.write(batch(&index, 0..=1_100, 1_200)).unwrap();
        assert_eq!(old.sealed_through(), Some(999));

        let common = BlockId::new(950, hash(950));
        let replacement = branch(common, 951..=1_110);
        let state =
            index.replace_deep_suffix(old.generation(), common, replacement, Some(1_010)).unwrap();

        assert_eq!(state.sealed_through(), Some(999));
        assert_eq!(index.read_block(state.generation(), 950).unwrap().hash, hash(950));
        assert_eq!(index.read_block(state.generation(), 951).unwrap().hash, branch_hash(951));
        assert!(index.read_block_by_hash(state.generation(), hash(951)).unwrap().is_none());
        index.verify_continuity().unwrap();
    }

    #[test]
    fn deep_reorg_at_a_range_boundary_keeps_the_shared_hashes() {
        let dir = tempfile::tempdir().unwrap();
        let index =
            Index::open(dir.path(), 20 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap();
        let old = index.write(batch(&index, 0..=1_100, 1_200)).unwrap();
        assert_eq!(old.sealed_through(), Some(999));

        // The affected range is the whole sealed range 0..=999, and every block in it is in both
        // the old and the new branch. The writer removes its hashes before it writes them again.
        let common = BlockId::new(999, hash(999));
        let replacement = branch(common, 1_000..=1_050);
        let state =
            index.replace_deep_suffix(old.generation(), common, replacement, Some(999)).unwrap();

        assert_eq!(state.sealed_through(), Some(999));
        for height in [0, 500, 999] {
            let block = index.read_block_by_hash(state.generation(), hash(height)).unwrap();
            assert_eq!(block.unwrap().height, height);
        }
        assert!(index.read_block_by_hash(state.generation(), hash(1_000)).unwrap().is_none());
        assert_eq!(index.read_block(state.generation(), 1_000).unwrap().hash, branch_hash(1_000));
        index.verify_continuity().unwrap();
    }

    fn batch(index: &Index, heights: std::ops::RangeInclusive<u32>, source_tip: u32) -> WriteBatch {
        let mut builder = OrderedBuilder::new(index.state().unwrap(), 10 * 1024 * 1024).unwrap();
        for height in heights {
            builder.push(prepared(height)).unwrap();
        }
        builder
            .build_batch(
                source_tip.checked_sub(PERSIST_DEPTH),
                source_tip.checked_sub(SEAL_DEPTH),
                10 * 1024 * 1024,
            )
            .unwrap()
            .unwrap()
    }

    /// A branch of empty blocks over `heights` that chains onto `ancestor`.
    fn branch(
        ancestor: BlockId,
        heights: std::ops::RangeInclusive<u32>,
    ) -> Vec<CompactBlockRecord> {
        let mut previous_hash = ancestor.hash;
        heights
            .map(|height| {
                let record = CompactBlockRecord {
                    height,
                    hash: branch_hash(height),
                    previous_hash,
                    time: height,
                    transactions: Vec::new(),
                    end_tree_sizes: TreeSizes::default(),
                };
                previous_hash = record.hash;
                record
            })
            .collect()
    }

    fn branch_hash(height: u32) -> block::Hash {
        let mut hash = hash(height);
        hash.0[31] = 1;
        hash
    }
}
