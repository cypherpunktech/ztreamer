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
//!   - An install of a [`VerifyState::Verified`] range at `verified_through + 1`, or at 0.
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
use zakura_chain::block;

#[cfg(any(test, feature = "test-utils"))]
use crate::codec::TreeSizes;
use crate::codec::{CompactBlockRecord, RANGE_FORMAT_VERSION, StoredBlock, decode_range_record};
use crate::ingest::{EncodedBatch, WriteBatch};

mod checked_range;
mod error;
mod metadata;
pub mod range_meta;
mod state;
mod tables;
mod telemetry;
mod writer;
pub use checked_range::{CheckedRange, RangeCheckError};
pub use error::IndexError;
use metadata::Metadata;
use range_meta::{RangeMeta, VerifyState};
pub use state::{BlockId, ChainIdentity, IndexState};
use tables::{RangeChain, SealedRange, Tables, range_start};
use telemetry::{WriteStage, record_state_metrics};
use writer::IndexWriter;

pub const SCHEMA_VERSION: u32 = 1;
pub const RANGE_SIZE: u32 = 1_000;
pub const PERSIST_DEPTH: u32 = 10;
pub const SEAL_DEPTH: u32 = 100;

/// The metadata row and the packed blob of one sealed range, from one LMDB snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedRangeWithMeta {
    /// The `range_meta` row of the range.
    pub meta: RangeMeta,
    /// The packed blob of the range, as in `sealed_ranges`.
    pub blob: Vec<u8>,
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

    /// Reads the metadata of the sealed range that starts at `start`, if any was written.
    pub fn range_meta(&self, start: u32) -> Result<Option<RangeMeta>, IndexError> {
        let txn = self.env.read_txn()?;
        self.tables.range_meta.get(&txn, start)
    }

    /// Writes the metadata of the sealed range that starts at `start`, replacing any earlier
    /// row. The range must be sealed, and `meta.entry` must describe exactly that range.
    ///
    /// The canonical block at `meta.entry.end_height` must have the hash `meta.entry.end_hash`,
    /// else the write returns [`IndexError::RangeEndHash`]. The check and the write are in one
    /// write transaction. Thus a deep reorg that replaces the range after the caller read it
    /// cannot get a row for the old blocks.
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
        if self.tables.hash_to_height.get(&txn, &meta.entry.end_hash)? != Some(end) {
            return Err(IndexError::RangeEndHash { start });
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

    /// Reads the `range_meta` row and the packed blob of the sealed range that starts at `start`
    /// from one generation-pinned LMDB snapshot. Thus a commit between two reads cannot give a
    /// row and a blob of different commits. Returns `None` if the range has no row.
    pub fn read_sealed_range_with_meta_at(
        &self,
        generation: u64,
        start: u32,
    ) -> Result<Option<SealedRangeWithMeta>, IndexError> {
        let txn = self.env.read_txn()?;
        self.read_generation(&txn, generation)?;
        let Some(meta) = self.tables.range_meta.get(&txn, start)? else {
            return Ok(None);
        };
        let blob = self.sealed_range_bytes_in(&txn, start)?;
        Ok(blob.map(|blob| SealedRangeWithMeta { meta, blob }))
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

    /// Sets the tip of an empty index to `tip`, with `tree_sizes`, as if sealed ranges cover
    /// `0..=tip.height`. The index holds no range. Thus it does not obey the coverage rule, and
    /// [`Index::open`] refuses it. For benches only: an install of the range after `tip` needs
    /// no ranges below it.
    ///
    /// # Panics
    ///
    /// Panics if the index has a state row or a block row, or if `tip.height` is not the last
    /// height of a range.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn set_sealed_tip_of_empty_index(
        &self,
        tip: BlockId,
        tree_sizes: TreeSizes,
    ) -> Result<IndexState, IndexError> {
        let mut txn = self.env.write_txn()?;
        assert_eq!(self.tables.metadata.get_state(&txn)?, IndexState::default());
        assert!(!self.tables.has_blocks(&txn)?, "the index holds blocks");
        assert_eq!(range_start(tip.height + 1), tip.height + 1, "the tip is not a range end");
        let state = IndexState {
            durable_tip: Some(tip),
            sealed_through: Some(tip.height),
            verified_through: None,
            generation: 1,
            tree_sizes,
        };
        self.tables.metadata.put_state(&mut txn, state)?;
        txn.commit()?;
        Ok(state)
    }

    /// Installs one packed range from a snapshot, and its metadata, in one write.
    ///
    /// Conditions for an install:
    ///
    /// - The index has no mutable rows.
    /// - `start` is `sealed_through + 1`, or 0 on an empty index. Thus ranges install in height
    ///   order, with no gaps.
    /// - The length and the BLAKE3 digest of the blob are `meta.entry.blob_len` and
    ///   `meta.entry.blob_digest`.
    /// - Each record of the blob decodes and links to the block below it. The first record links
    ///   to the tip, or to the genesis previous hash for range 0.
    /// - The tree sizes do not decrease from the tip to the last record.
    /// - No block hash of the blob is in the index already, and no hash occurs two times.
    /// - `meta.entry` gives the range format version, the heights, the end hash and the end tree
    ///   sizes of the blob.
    /// - `meta.state` is not [`VerifyState::Rejected`].
    ///
    /// The method does the checks on the state first. Then it checks the length and the digest.
    /// Then it decodes the blob. It calculates the digest before it starts the write
    /// transaction, because the digest does not need the index.
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
        let blob_check = meta.entry.check_blob(blob);
        let writer = self.start_install(start)?;
        blob_check.map_err(|mismatch| IndexError::Blob { start, mismatch })?;
        let state = writer.state();
        let chain = SealedRange::new(start, blob)?
            .check_chain(state.install_parent_hash(), state.tree_sizes)?;
        Self::finish_install(writer, start, blob, &chain, meta)
    }

    /// Installs a range that [`CheckedRange::check`] checked, and its metadata, in one write.
    ///
    /// The conditions are the conditions of [`Index::install_sealed_range`]. The
    /// [`CheckedRange`] proves the conditions that do not need the index. Thus this method does
    /// only the checks that need the index, and calculates no digest and decodes no record:
    ///
    /// - The index has no mutable rows, and the range starts at `sealed_through + 1`, or at 0 on
    ///   an empty index.
    /// - `meta.entry.blob_len` and `meta.entry.blob_digest` are the length and the digest that
    ///   the check of the range compared.
    /// - The first record links to the tip, or to the genesis previous hash for range 0, and
    ///   its tree sizes are not smaller than the tree sizes of the tip.
    /// - `meta` describes the range, and no block hash of the range is in the index already.
    ///
    /// The write is the write of [`Index::install_sealed_range`].
    pub fn install_checked_range(
        &self,
        range: &CheckedRange,
        meta: &RangeMeta,
    ) -> Result<IndexState, IndexError> {
        let start = range.start();
        let writer = self.start_install(start)?;
        range
            .check_entry_blob(&meta.entry)
            .map_err(|mismatch| IndexError::Blob { start, mismatch })?;
        let (state, chain) = (writer.state(), range.chain());
        if !chain.links_to(state.install_parent_hash(), state.tree_sizes) {
            return Err(IndexError::RangeBlob { height: start });
        }
        Self::finish_install(writer, start, range.blob(), chain, meta)
    }

    /// Starts the write of an install at `start`. Refuses an install that is not at the next
    /// range start, and an index with mutable rows.
    fn start_install(&self, start: u32) -> Result<IndexWriter<'_>, IndexError> {
        let writer = IndexWriter::start_latest(&self.env, self.tables)?;
        let state = writer.state();
        let next = state.next_range_start()?;
        if start != next {
            return Err(IndexError::RangeOrder { start, next });
        }
        if state.has_mutable_rows() {
            return Err(IndexError::MutableRows);
        }
        Ok(writer)
    }

    /// Checks that `meta` describes the range `chain` at `start`, then writes the range, its
    /// hash entries and `meta`, and moves the tip to the range end.
    fn finish_install(
        mut writer: IndexWriter<'_>,
        start: u32,
        blob: &[u8],
        chain: &RangeChain,
        meta: &RangeMeta,
    ) -> Result<IndexState, IndexError> {
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
mod tests;
