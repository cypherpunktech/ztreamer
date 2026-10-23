//! Generation-pinned LMDB reads, atomic writes, packed ranges, and reorg replacement.
//!
//! # Tables
//!
//! The [`Index`] keeps the chain in four LMDB databases in one environment. All integers are
//! big-endian, so LMDB keeps the height keys in height order.
//!
//! ```text
//! table           key                    value
//! metadata        "state"                IndexState, 62 bytes
//!                 "identity"             chain identity and fixed policy
//! sealed_ranges   first height, u32      one packed range of RANGE_SIZE records
//! mutable_blocks  height, u32            one encoded CompactBlockRecord
//! hash_to_height  block hash, 32 bytes   height, u32
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
//! - An empty index has no state row, and its three block tables are empty.
//!
//! # Writes and generations
//!
//! Each write is one LMDB write transaction. It changes the tables, then it commits. If a write
//! fails, LMDB discards all of its changes. Each commit increases the generation by 1.
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
//! - the tip hash and the tree sizes in the state.
//!
//! It does not decode the blocks inside a range. Thus it does not examine their chain links or
//! their hash entries. The range encoder checks the chain links when it writes the range.
//!
//! # Value formats
//!
//! - A mutable row is the encoding of one [`CompactBlockRecord`]:
//!   `version (u8) | height | hash (32 bytes) | previous hash (32 bytes) | length | protobuf
//!   CompactBlock`.
//! - A sealed range holds [`RANGE_SIZE`] records in the same encoding, and an offset table. A
//!   reader can decode one record and not the others. [`encode_range`](crate::codec::encode_range)
//!   writes a range. The `write_range` function in `codec.rs` gives its byte layout.
//! - `index/metadata.rs` gives the byte layout of the state and of the identity.

use std::{fs, io, path::Path, time::Instant};

use heed::{Env, EnvOpenOptions, RoTxn};

use crate::codec::{CodecError, CompactBlockRecord, StoredBlock, TreeSizes, decode_range_record};
use crate::{
    Digest,
    ingest::{EncodedBatch, WriteBatch},
};

mod metadata;
mod tables;
mod telemetry;
mod writer;
use tables::{Tables, range_start};
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
    pub hash: Digest,
}

impl BlockId {
    pub const fn new(height: u32, hash: Digest) -> Self {
        Self { height, hash }
    }
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

    /// Identifies the exact committed revision of the durable index that an
    /// [`IndexState`] describes.
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Cumulative [`TreeSizes`] at the end of [`IndexState::durable_tip`]
    pub const fn tree_sizes(&self) -> TreeSizes {
        self.tree_sizes
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
}

/// Ztreamer's four-database LMDB index.
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
        genesis_hash: Digest,
    ) -> Result<Self, IndexError> {
        fs::create_dir_all(path.as_ref())?;
        // SAFETY: callers must not open this path with incompatible LMDB options in this process.
        let env =
            unsafe { EnvOpenOptions::new().map_size(map_size).max_dbs(4).open(path.as_ref())? };
        let mut txn = env.write_txn()?;
        let tables = Tables::open(&env, &mut txn)?;
        tables.metadata.put_or_check_identity(&mut txn, network, genesis_hash)?;
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
        hash: Digest,
    ) -> Result<Option<CompactBlockRecord>, IndexError> {
        self.read_block_by_hash_as(generation, hash)
    }

    /// Resolves and reads one canonical block hash from a generation-pinned LMDB snapshot.
    pub fn read_block_by_hash_as<T: StoredBlock>(
        &self,
        generation: u64,
        hash: Digest,
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
        let Tables { metadata, sealed_ranges, mutable_blocks, hash_to_height } = self.tables;
        let state = metadata.get_state(&txn)?;
        let Some(tip) = state.durable_tip else {
            if self.tables.has_blocks(&txn)? {
                return Err(IndexError::Metadata);
            }
            return Ok(());
        };

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
        writer.seal_batch_ranges(seal_through, &mut blocks)?;
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
        writer.seal_mutable_ranges(seal_through)?;
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
        writer.seal_mutable_ranges(seal_through)?;
        writer.commit()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{hash, prepared};
    use crate::{
        codec::{ProtobufBlockRecord, decode_range_record},
        ingest::OrderedBuilder,
    };

    #[test]
    fn creates_four_databases_and_checks_chain_identity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index");

        drop(Index::open(&path, 10 * 1024 * 1024, "Mainnet", [1; 32]).unwrap());
        drop(Index::open(&path, 10 * 1024 * 1024, "Mainnet", [1; 32]).unwrap());
        assert!(Index::open(&path, 10 * 1024 * 1024, "Testnet", [1; 32]).is_err());
        assert!(Index::open(&path, 10 * 1024 * 1024, "Mainnet", [2; 32]).is_err());
    }

    #[test]
    fn atomically_writes_restarts_and_packs_sealed_ranges() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index");
        let index = Index::open(&path, 10 * 1024 * 1024, "Mainnet", [1; 32]).unwrap();
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
        let reopened = Index::open(&path, 10 * 1024 * 1024, "Mainnet", [1; 32]).unwrap();
        assert_eq!(reopened.state().unwrap(), state);
    }

    #[test]
    fn seals_a_range_that_is_complete_in_mutable_rows() {
        let dir = tempfile::tempdir().unwrap();
        let index = Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", [1; 32]).unwrap();
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
        let index = Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", [1; 32]).unwrap();
        // This write seals no range.
        let stages = write_stages(&index, batch(&index, 0..=10, 20));
        assert!(stages.iter().all(|stage| !stage.starts_with("seal_")));
        assert!(stages.contains(&"mutable_put".to_owned()));
        assert!(stages.contains(&"state_put".to_owned()));

        // This write seals a range from its batch only. It reads and writes no mutable rows.
        let dir = tempfile::tempdir().unwrap();
        let index = Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", [1; 32]).unwrap();
        let stages = write_stages(&index, batch(&index, 0..=999, 1_100));
        assert!(stages.contains(&"seal_encode".to_owned()));
        assert!(stages.contains(&"seal_put".to_owned()));
        assert!(!stages.contains(&"seal_read".to_owned()));
        assert!(!stages.contains(&"mutable_put".to_owned()));
    }

    #[test]
    fn seal_fails_for_a_damaged_mutable_row() {
        let dir = tempfile::tempdir().unwrap();
        let index = Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", [1; 32]).unwrap();
        index.write(batch(&index, 0..=10, 20)).unwrap();

        // Change the hash in the payload. The hash in the envelope stays correct.
        let mut txn = index.env.write_txn().unwrap();
        let mut row = index.tables.mutable_blocks.get(&txn, 5).unwrap().unwrap().to_vec();
        let payload_hash = row.windows(32).rposition(|w| w == hash(5)).unwrap();
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
        let index = Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", [1; 32]).unwrap();
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
        let envelope = 1 + size_of::<u32>() + 2 * size_of::<Digest>();
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
        let index = Index::open(&path, 10 * 1024 * 1024, "Mainnet", [1; 32]).unwrap();
        index.write(batch(&index, 0..=10, 20)).unwrap();
        let mut txn = index.env.write_txn().unwrap();
        index.tables.mutable_blocks.delete(&mut txn, 5).unwrap();
        txn.commit().unwrap();
        drop(index);

        assert!(matches!(
            Index::open(&path, 10 * 1024 * 1024, "Mainnet", [1; 32]),
            Err(IndexError::Continuity { .. })
        ));
    }

    #[test]
    fn persistence_and_sealing_use_exact_depth_boundaries() {
        let dir = tempfile::tempdir().unwrap();
        let index = Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", [1; 32]).unwrap();
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
        let index = Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", [1; 32]).unwrap();
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
        let index = Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", [1; 32]).unwrap();
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
        let index = Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", [1; 32]).unwrap();
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
    fn deep_reorg_rebuilds_the_affected_sealed_range() {
        let dir = tempfile::tempdir().unwrap();
        let index = Index::open(dir.path(), 20 * 1024 * 1024, "Mainnet", [1; 32]).unwrap();
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
        let index = Index::open(dir.path(), 20 * 1024 * 1024, "Mainnet", [1; 32]).unwrap();
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

    fn branch_hash(height: u32) -> Digest {
        let mut hash = hash(height);
        hash[31] = 1;
        hash
    }
}
