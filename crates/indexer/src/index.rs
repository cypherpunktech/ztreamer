//! Generation-pinned LMDB reads, atomic writes, packed ranges, and reorg replacement.

use std::{
    fs, io,
    path::Path,
    time::{Duration, Instant},
};

use heed::{
    Database, Env, EnvOpenOptions, RoTxn, RwTxn,
    byteorder::BigEndian,
    types::{Bytes, U32},
};

use crate::codec::{
    CodecError, CompactBlockRecord, RangeDecoder, StoredBlock, TreeSizes, decode_range_record,
    encode_range_from_bytes,
};
use crate::{
    Digest,
    ingest::{EncodedBatch, WriteBatch},
};

pub const SCHEMA_VERSION: u32 = 1;
pub const RANGE_SIZE: u32 = 1_000;
pub const PERSIST_DEPTH: u32 = 10;
pub const SEAL_DEPTH: u32 = 100;

const STATE_FORMAT_VERSION: u8 = 1;
const STATE_BYTES: usize = 62;
const STATE: &[u8] = b"state";

type HeightDb = Database<U32<BigEndian>, Bytes>;

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
    /// The highest contiguous compact block commited to [`Index::mutable_blocks`]
    pub(crate) durable_tip: Option<BlockId>,
    /// The highest height committed to a [`Index::sealed_ranges`].
    ///
    /// Blocks above this height live in [`Index::mutable_blocks`].
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
    metadata: Database<Bytes, Bytes>,
    sealed_ranges: HeightDb,
    mutable_blocks: HeightDb,
    hash_to_height: Database<Bytes, U32<BigEndian>>,
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
        let metadata = env.create_database(&mut txn, Some("metadata"))?;
        let sealed_ranges = env.create_database(&mut txn, Some("sealed_ranges"))?;
        let mutable_blocks = env.create_database(&mut txn, Some("mutable_blocks"))?;
        let hash_to_height = env.create_database(&mut txn, Some("hash_to_height"))?;

        let identity = identity(network, genesis_hash);
        match metadata.get(&txn, b"identity".as_slice())? {
            Some(stored) if stored != identity => return Err(IndexError::Identity),
            None => metadata.put(&mut txn, b"identity".as_slice(), identity.as_slice())?,
            Some(_) => {}
        }
        txn.commit()?;

        let index = Self { env, metadata, sealed_ranges, mutable_blocks, hash_to_height };
        index.verify_continuity()?;
        record_state_metrics(index.state()?);
        Ok(index)
    }

    pub fn state(&self) -> Result<IndexState, IndexError> {
        let txn = self.env.read_txn()?;
        read_state(self.metadata, &txn)
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
        self.read_record(&txn, state, height)
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
        self.hash_to_height
            .get(&txn, hash.as_slice())?
            .map(|height| self.read_record(&txn, state, height))
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
        let state = read_state(self.metadata, &txn)?;
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
                let range_start = height - height % RANGE_SIZE;
                let bytes = self
                    .sealed_ranges
                    .get(txn, &range_start)?
                    .ok_or(IndexError::Coverage { height })?;
                let range = RangeDecoder::new(bytes)?;
                let chunk_end = if ascending {
                    end.min(range_start + RANGE_SIZE - 1)
                } else {
                    end.max(range_start)
                };
                loop {
                    if !emit(range.record_as((height - range_start) as usize)?) {
                        return Ok(());
                    }
                    if height == chunk_end {
                        break;
                    }
                    height = if ascending { height + 1 } else { height - 1 };
                }
            } else if !emit(self.read_record(txn, state, height)?) {
                return Ok(());
            }

            if height == end {
                return Ok(());
            }
            height = if ascending { height + 1 } else { height - 1 };
        }
    }

    fn read_generation(&self, txn: &RoTxn<'_>, generation: u64) -> Result<IndexState, IndexError> {
        let state = read_state(self.metadata, txn)?;
        if state.generation != generation {
            return Err(IndexError::Generation { expected: generation, actual: state.generation });
        }
        Ok(state)
    }

    fn read_record<T: StoredBlock>(
        &self,
        txn: &RoTxn<'_>,
        state: IndexState,
        height: u32,
    ) -> Result<T, IndexError> {
        if state.durable_tip.is_none_or(|tip| height > tip.height) {
            return Err(IndexError::Coverage { height });
        }
        if state.sealed_through.is_some_and(|sealed| height <= sealed) {
            let start = height - height % RANGE_SIZE;
            return RangeDecoder::new(
                self.sealed_ranges.get(txn, &start)?.ok_or(IndexError::Coverage { height })?,
            )?
            .record_as((height - start) as usize)
            .map_err(Into::into);
        }
        T::decode(self.mutable_blocks.get(txn, &height)?.ok_or(IndexError::Coverage { height })?)
            .map_err(Into::into)
    }

    /// Checks sealed-range summaries and every mutable suffix row without rescanning sealed blocks.
    pub fn verify_continuity(&self) -> Result<(), IndexError> {
        let txn = self.env.read_txn()?;
        let state = read_state(self.metadata, &txn)?;
        let Some(tip) = state.durable_tip else {
            if !self.sealed_ranges.is_empty(&txn)?
                || !self.mutable_blocks.is_empty(&txn)?
                || !self.hash_to_height.is_empty(&txn)?
            {
                return Err(IndexError::Metadata);
            }
            return Ok(());
        };

        let sealed_blocks = state.sealed_through.map_or(0, |height| u64::from(height) + 1);
        if self.sealed_ranges.len(&txn)? != sealed_blocks / u64::from(RANGE_SIZE)
            || self.mutable_blocks.len(&txn)? != u64::from(tip.height) + 1 - sealed_blocks
            || self.hash_to_height.len(&txn)? != u64::from(tip.height) + 1
        {
            return Err(IndexError::Continuity { height: 0 });
        }

        let mut next_height = 0u64;
        let mut previous_hash = None;
        for entry in self.sealed_ranges.iter(&txn)? {
            let (start, bytes) = entry?;
            if u64::from(start) != next_height {
                return Err(IndexError::Continuity { height: next_height as u32 });
            }
            let first = decode_range_record(bytes, 0)?;
            let last = decode_range_record(bytes, RANGE_SIZE as usize - 1)?;
            if previous_hash.is_some_and(|hash| first.previous_hash != hash) {
                return Err(IndexError::Continuity { height: start });
            }
            self.verify_hash_entry(&txn, &first)?;
            self.verify_hash_entry(&txn, &last)?;
            previous_hash = Some(last.hash);
            next_height += u64::from(RANGE_SIZE);
        }
        if next_height != sealed_blocks {
            return Err(IndexError::Continuity { height: next_height as u32 });
        }

        for entry in self.mutable_blocks.iter(&txn)? {
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
            self.verify_hash_entry(&txn, &record)?;
            previous_hash = Some(record.hash);
            next_height += 1;
        }
        let tip_record = if state.sealed_through.is_some_and(|sealed| tip.height <= sealed) {
            let start = tip.height - tip.height % RANGE_SIZE;
            decode_range_record(
                self.sealed_ranges
                    .get(&txn, &start)?
                    .ok_or(IndexError::Continuity { height: tip.height })?,
                (tip.height - start) as usize,
            )?
        } else {
            CompactBlockRecord::decode(
                self.mutable_blocks
                    .get(&txn, &tip.height)?
                    .ok_or(IndexError::Continuity { height: tip.height })?,
            )?
        };
        if next_height != u64::from(tip.height) + 1
            || previous_hash != Some(tip.hash)
            || state.tree_sizes != tip_record.end_tree_sizes
        {
            return Err(IndexError::Continuity { height: tip.height });
        }
        Ok(())
    }

    fn verify_hash_entry(
        &self,
        txn: &RoTxn<'_>,
        record: &CompactBlockRecord,
    ) -> Result<(), IndexError> {
        if self.hash_to_height.get(txn, record.hash.as_slice())? != Some(record.height) {
            return Err(IndexError::HashIndex { height: record.height });
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
        let mut txn = self.env.write_txn()?;
        let mut state = read_state(self.metadata, &txn)?;
        let EncodedBatch { base_generation, seal_through, blocks, end_tree_sizes } = batch;
        let first = blocks.first().expect("builders never emit empty batches");
        let expected_height = match state.durable_tip {
            Some(tip) => tip.height.checked_add(1).ok_or(IndexError::Overflow)?,
            None => 0,
        };
        if state.generation != base_generation
            || first.height != expected_height
            || state.durable_tip.is_some_and(|tip| first.previous_hash != tip.hash)
        {
            return Err(IndexError::StaleBatch { batch_generation: base_generation });
        }

        let mut timer = WriteTimer::start();
        for block in &blocks {
            self.hash_to_height.put(&mut txn, block.hash.as_slice(), &block.height)?;
        }
        timer.lap(WriteStage::HashIndex);
        let last = blocks.last().expect("the batch is non-empty");
        let tip = BlockId::new(last.height, last.hash);
        let mut blocks = blocks.into_iter().peekable();
        let mut start = state
            .sealed_through
            .map_or(Ok(0), |height| height.checked_add(1).ok_or(IndexError::Overflow))?;

        while let Some(seal_cutoff) = seal_through {
            let end = start.checked_add(RANGE_SIZE - 1).ok_or(IndexError::Overflow)?;
            if end > seal_cutoff || end > tip.height {
                break;
            }
            // The head follower writes blocks to `mutable_blocks` before it seals them. Because of
            // this, the range can be complete in `mutable_blocks`, and the batch can start after it.
            let first_new = blocks.peek().map_or(end + 1, |block| block.height);
            let mutable_end = first_new.min(end + 1);
            let mut range = self.read_rows_to_seal(&txn, start..mutable_end, start)?;
            timer.seal_read(range.len());
            while range.len() < RANGE_SIZE as usize {
                let block = blocks.next().ok_or(IndexError::IncompleteRange { start })?;
                range.push(block.bytes);
            }
            let parts: Vec<_> = range.iter().map(Vec::as_slice).collect();
            let packed = encode_range_from_bytes(&parts)?;
            drop(parts);
            drop(range);
            timer.lap(WriteStage::SealEncode);
            self.sealed_ranges.put(&mut txn, &start, packed.as_slice())?;
            for height in start..mutable_end {
                self.mutable_blocks.delete(&mut txn, &height)?;
            }
            timer.lap(WriteStage::SealPut);
            timer.bytes(WriteStage::SealPut, packed.len());
            state.sealed_through = Some(end);
            start = end.checked_add(1).ok_or(IndexError::Overflow)?;
        }
        let wrote_mutable = blocks.peek().is_some();
        for block in blocks {
            self.mutable_blocks.put(&mut txn, &block.height, block.bytes.as_slice())?;
            timer.bytes(WriteStage::MutablePut, block.bytes.len());
        }
        timer.lap_if(wrote_mutable, WriteStage::MutablePut);
        state.durable_tip = Some(tip);
        state.tree_sizes = end_tree_sizes;
        state.generation = state.generation.checked_add(1).ok_or(IndexError::Overflow)?;
        let encoded = encode_state(state);
        self.metadata.put(&mut txn, STATE, encoded.as_slice())?;
        timer.lap(WriteStage::StatePut);
        txn.commit()?;
        timer.lap(WriteStage::Commit);
        timer.finish();
        record_state_metrics(state);
        Ok(state)
    }

    /// Atomically replaces every mutable block after `ancestor`.
    pub fn replace_mutable_suffix(
        &self,
        base_generation: u64,
        ancestor: BlockId,
        records: Vec<CompactBlockRecord>,
        seal_through: Option<u32>,
    ) -> Result<IndexState, IndexError> {
        let mut txn = self.env.write_txn()?;
        let mut state = read_state(self.metadata, &txn)?;
        if state.generation != base_generation {
            return Err(IndexError::StaleBatch { batch_generation: base_generation });
        }
        if state.sealed_through.is_some_and(|sealed| ancestor.height < sealed) {
            return Err(IndexError::Sealed { height: ancestor.height + 1 });
        }
        let ancestor_record: CompactBlockRecord = self.read_record(&txn, state, ancestor.height)?;
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
        for height in delete_start..=old_tip.height {
            let bytes =
                self.mutable_blocks.get(&txn, &height)?.ok_or(IndexError::Continuity { height })?;
            let old = CompactBlockRecord::decode(bytes)?;
            self.hash_to_height.delete(&mut txn, old.hash.as_slice())?;
            self.mutable_blocks.delete(&mut txn, &height)?;
        }
        for record in &records {
            let encoded = record.encode()?;
            self.mutable_blocks.put(&mut txn, &record.height, encoded.as_slice())?;
            self.hash_to_height.put(&mut txn, record.hash.as_slice(), &record.height)?;
        }

        let tip =
            records.last().map_or(ancestor, |record| BlockId::new(record.height, record.hash));
        state.durable_tip = Some(tip);
        state.tree_sizes =
            records.last().map_or(ancestor_record.end_tree_sizes, |record| record.end_tree_sizes);
        self.pack_sealed_ranges(&mut txn, seal_through, &mut state)?;
        state.generation = state.generation.checked_add(1).ok_or(IndexError::Overflow)?;
        let encoded = encode_state(state);
        self.metadata.put(&mut txn, STATE, encoded.as_slice())?;
        txn.commit()?;
        record_state_metrics(state);
        Ok(state)
    }

    /// Atomically rebuilds every physical range touched by a deep reorg.
    pub fn replace_deep_suffix(
        &self,
        base_generation: u64,
        common_ancestor: BlockId,
        replacement: Vec<CompactBlockRecord>,
        seal_through: Option<u32>,
    ) -> Result<IndexState, IndexError> {
        let mut txn = self.env.write_txn()?;
        let mut state = read_state(self.metadata, &txn)?;
        if state.generation != base_generation {
            return Err(IndexError::StaleBatch { batch_generation: base_generation });
        }
        let old_tip = state.durable_tip.ok_or(IndexError::Metadata)?;
        let common: CompactBlockRecord = self.read_record(&txn, state, common_ancestor.height)?;
        if common.hash != common_ancestor.hash {
            return Err(IndexError::Replacement { height: common_ancestor.height });
        }

        let affected_start = common_ancestor.height - common_ancestor.height % RANGE_SIZE;
        let old_records = (affected_start..=old_tip.height)
            .map(|height| self.read_record(&txn, state, height))
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

        for record in &old_records {
            self.hash_to_height.delete(&mut txn, record.hash.as_slice())?;
            self.mutable_blocks.delete(&mut txn, &record.height)?;
        }
        if let Some(old_sealed) = state.sealed_through
            && affected_start <= old_sealed
        {
            let mut start = affected_start;
            while start <= old_sealed {
                self.sealed_ranges.delete(&mut txn, &start)?;
                start = start.checked_add(RANGE_SIZE).ok_or(IndexError::Overflow)?;
            }
            state.sealed_through =
                affected_start.checked_sub(1).filter(|height| *height <= old_sealed);
        }

        for record in &canonical {
            let encoded = record.encode()?;
            self.mutable_blocks.put(&mut txn, &record.height, encoded.as_slice())?;
            self.hash_to_height.put(&mut txn, record.hash.as_slice(), &record.height)?;
        }
        let tip = canonical.last().expect("the common-ancestor prefix is non-empty");
        state.durable_tip = Some(BlockId::new(tip.height, tip.hash));
        state.tree_sizes = tip.end_tree_sizes;
        self.pack_sealed_ranges(&mut txn, seal_through, &mut state)?;
        state.generation = state.generation.checked_add(1).ok_or(IndexError::Overflow)?;
        let encoded = encode_state(state);
        self.metadata.put(&mut txn, STATE, encoded.as_slice())?;
        txn.commit()?;
        record_state_metrics(state);
        Ok(state)
    }

    /// Reads the mutable rows of `heights` for a seal of the range at `start`. A mutable row
    /// holds the same record bytes that a range holds, so this copies the bytes and does not
    /// encode them again. It decodes and checks each row once, with the same rules as
    /// [`CompactBlockRecord::encode`], so that a damaged row does not go into a sealed range.
    fn read_rows_to_seal(
        &self,
        txn: &RoTxn<'_>,
        heights: std::ops::Range<u32>,
        start: u32,
    ) -> Result<Vec<Vec<u8>>, IndexError> {
        heights
            .map(|height| {
                let bytes = self
                    .mutable_blocks
                    .get(txn, &height)?
                    .ok_or(IndexError::IncompleteRange { start })?;
                CompactBlockRecord::decode(bytes)?.check()?;
                Ok(bytes.to_vec())
            })
            .collect()
    }

    fn pack_sealed_ranges(
        &self,
        txn: &mut RwTxn<'_>,
        seal_cutoff: Option<u32>,
        state: &mut IndexState,
    ) -> Result<(), IndexError> {
        let Some(seal_cutoff) = seal_cutoff else {
            return Ok(());
        };
        let mut start = state
            .sealed_through
            .map_or(Ok(0), |height| height.checked_add(1).ok_or(IndexError::Overflow))?;

        loop {
            let end = start.checked_add(RANGE_SIZE - 1).ok_or(IndexError::Overflow)?;
            // exit early if the range is not ready to be sealed yet
            if end > seal_cutoff
                || state.durable_tip.is_none_or(|durable_tip| end > durable_tip.height)
            {
                break;
            }
            let range = self.read_rows_to_seal(txn, start..end + 1, start)?;
            let parts: Vec<_> = range.iter().map(Vec::as_slice).collect();
            let encoded = encode_range_from_bytes(&parts)?;
            self.sealed_ranges.put(txn, &start, encoded.as_slice())?;
            for height in start..=end {
                self.mutable_blocks.delete(txn, &height)?;
            }
            state.sealed_through = Some(end);
            start = end.checked_add(1).ok_or(IndexError::Overflow)?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum WriteStage {
    Encode,
    HashIndex,
    SealRead,
    SealEncode,
    SealPut,
    MutablePut,
    StatePut,
    Commit,
}

const WRITE_STAGES: usize = 8;

impl WriteStage {
    const ALL: [Self; WRITE_STAGES] = [
        Self::Encode,
        Self::HashIndex,
        Self::SealRead,
        Self::SealEncode,
        Self::SealPut,
        Self::MutablePut,
        Self::StatePut,
        Self::Commit,
    ];

    fn record(self, elapsed: Duration) {
        metrics::histogram!("ztreamer.index.write.seconds", "stage" => self.label())
            .record(elapsed.as_secs_f64());
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Encode => "encode",
            Self::HashIndex => "hash_index",
            Self::SealRead => "seal_read",
            Self::SealEncode => "seal_encode",
            Self::SealPut => "seal_put",
            Self::MutablePut => "mutable_put",
            Self::StatePut => "state_put",
            Self::Commit => "commit",
        }
    }
}

/// Measures the stages of one [`Index::write`]. [`WriteTimer::finish`] records the times and the
/// bytes after the commit. A write that fails records nothing here, because its transaction
/// writes nothing: the pipeline commit metric counts it with the outcome `error`. A stage that
/// did not run, for example a seal, has no sample.
struct WriteTimer {
    last: Instant,
    elapsed: [Option<Duration>; WRITE_STAGES],
    bytes: [usize; WRITE_STAGES],
    seal_read_blocks: usize,
}

impl WriteTimer {
    fn start() -> Self {
        Self {
            last: Instant::now(),
            elapsed: [None; WRITE_STAGES],
            bytes: [0; WRITE_STAGES],
            seal_read_blocks: 0,
        }
    }

    fn lap(&mut self, stage: WriteStage) {
        let now = Instant::now();
        *self.elapsed[stage as usize].get_or_insert(Duration::ZERO) += now - self.last;
        self.last = now;
    }

    /// Records the stage only if it did work. Else the time goes to no stage.
    fn lap_if(&mut self, ran: bool, stage: WriteStage) {
        if ran {
            self.lap(stage);
        } else {
            self.last = Instant::now();
        }
    }

    fn seal_read(&mut self, blocks: usize) {
        self.lap_if(blocks > 0, WriteStage::SealRead);
        self.seal_read_blocks += blocks;
    }

    const fn bytes(&mut self, stage: WriteStage, bytes: usize) {
        self.bytes[stage as usize] += bytes;
    }
}

impl WriteTimer {
    /// Records the stages of a write that committed.
    fn finish(self) {
        for stage in WriteStage::ALL {
            let index = stage as usize;
            if let Some(elapsed) = self.elapsed[index] {
                stage.record(elapsed);
            }
            if self.bytes[index] > 0 {
                metrics::counter!("ztreamer.index.write.bytes", "stage" => stage.label())
                    .increment(self.bytes[index] as u64);
            }
        }
        metrics::counter!("ztreamer.index.write.seal_read.blocks")
            .increment(self.seal_read_blocks as u64);
    }
}

fn record_state_metrics(state: IndexState) {
    metrics::gauge!("ztreamer.index.durable.block.height")
        .set(state.durable_tip.map_or(f64::NAN, |tip| tip.height.into()));
    metrics::gauge!("ztreamer.index.sealed.block.height")
        .set(state.sealed_through.map_or(f64::NAN, Into::into));
    metrics::gauge!("ztreamer.index.generation").set(state.generation as f64);
}

fn read_state(metadata: Database<Bytes, Bytes>, txn: &RoTxn<'_>) -> Result<IndexState, IndexError> {
    metadata.get(txn, STATE)?.map(decode_state).transpose().map(Option::unwrap_or_default)
}

fn encode_state(state: IndexState) -> Vec<u8> {
    let tip = state.durable_tip.expect("only non-empty index state is persisted");
    let mut bytes = Vec::with_capacity(STATE_BYTES);
    bytes.push(STATE_FORMAT_VERSION);
    bytes.extend_from_slice(&tip.height.to_be_bytes());
    bytes.extend_from_slice(&tip.hash);
    bytes.push(u8::from(state.sealed_through.is_some()));
    bytes.extend_from_slice(&state.sealed_through.unwrap_or_default().to_be_bytes());
    bytes.extend_from_slice(&state.generation.to_be_bytes());
    bytes.extend_from_slice(&state.tree_sizes.sapling.to_be_bytes());
    bytes.extend_from_slice(&state.tree_sizes.orchard.to_be_bytes());
    bytes.extend_from_slice(&state.tree_sizes.ironwood.to_be_bytes());
    bytes
}

fn decode_state(bytes: &[u8]) -> Result<IndexState, IndexError> {
    if bytes.len() != STATE_BYTES || bytes[0] != STATE_FORMAT_VERSION || bytes[37] > 1 {
        return Err(IndexError::Metadata);
    }
    let u32_at = |start| {
        u32::from_be_bytes(bytes[start..start + 4].try_into().expect("state length was checked"))
    };
    let tip = BlockId::new(u32_at(1), bytes[5..37].try_into().expect("state length was checked"));
    let sealed_through = (bytes[37] == 1).then(|| u32_at(38));
    if sealed_through
        .is_some_and(|sealed| sealed > tip.height || sealed % RANGE_SIZE != RANGE_SIZE - 1)
    {
        return Err(IndexError::Metadata);
    }
    Ok(IndexState {
        durable_tip: Some(tip),
        sealed_through,
        generation: u64::from_be_bytes(bytes[42..50].try_into().expect("state length was checked")),
        tree_sizes: TreeSizes { sapling: u32_at(50), orchard: u32_at(54), ironwood: u32_at(58) },
    })
}

fn identity(network: &str, genesis_hash: Digest) -> Vec<u8> {
    [
        SCHEMA_VERSION.to_be_bytes().as_slice(),
        RANGE_SIZE.to_be_bytes().as_slice(),
        PERSIST_DEPTH.to_be_bytes().as_slice(),
        SEAL_DEPTH.to_be_bytes().as_slice(),
        genesis_hash.as_slice(),
        network.as_bytes(),
    ]
    .concat()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        codec::{ProtobufBlockRecord, decode_range_record},
        ingest::OrderedBuilder,
        parser::ParsedCompactBlock,
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
        assert!(index.mutable_blocks.get(&txn, &0).unwrap().is_none());
        assert!(index.mutable_blocks.get(&txn, &1_000).unwrap().is_some());
        let range = index.sealed_ranges.get(&txn, &0).unwrap().unwrap();
        assert_eq!(decode_range_record(range, 537).unwrap().height, 537);
        assert_eq!(index.hash_to_height.get(&txn, hash(537).as_slice()).unwrap(), Some(537));
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
        assert!(index.mutable_blocks.get(&txn, &999).unwrap().is_none());
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
        let mut row = index.mutable_blocks.get(&txn, &5).unwrap().unwrap().to_vec();
        let payload_hash = row.windows(32).rposition(|w| w == hash(5)).unwrap();
        row[payload_hash] ^= 1;
        index.mutable_blocks.put(&mut txn, &5, &row).unwrap();
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
        let row = index.mutable_blocks.get(&txn, &5).unwrap().unwrap().to_vec();
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
        index.mutable_blocks.put(&mut txn, &5, &damaged).unwrap();
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
        index.mutable_blocks.delete(&mut txn, &5).unwrap();
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
        assert!(index.mutable_blocks.get(&txn, &0).unwrap().is_none());
        drop(txn);

        let state = index
            .write(builder.build_batch(Some(0), None, 10 * 1024 * 1024).unwrap().unwrap())
            .unwrap();
        let txn = index.env.read_txn().unwrap();
        assert!(index.mutable_blocks.get(&txn, &0).unwrap().is_some());
        drop(txn);

        let mut builder = OrderedBuilder::new(state, 10 * 1024 * 1024).unwrap();
        for height in 1..=90 {
            builder.push(prepared(height)).unwrap();
        }
        let state = index
            .write(builder.build_batch(Some(90), Some(0), 10 * 1024 * 1024).unwrap().unwrap())
            .unwrap();
        let txn = index.env.read_txn().unwrap();
        assert!(index.mutable_blocks.get(&txn, &0).unwrap().is_some());
        assert!(index.sealed_ranges.get(&txn, &0).unwrap().is_none());
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
        assert!(index.mutable_blocks.get(&txn, &0).unwrap().is_none());
        assert!(index.sealed_ranges.get(&txn, &0).unwrap().is_some());
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
                    index.mutable_blocks.get(&txn, &height).unwrap().unwrap(),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let encoded = crate::codec::encode_range(&records).unwrap();
        index.sealed_ranges.put(&mut txn, &0, encoded.as_slice()).unwrap();
        for height in 0..RANGE_SIZE / 2 {
            index.mutable_blocks.delete(&mut txn, &height).unwrap();
        }
        drop(txn); // simulated failure before commit

        let txn = index.env.read_txn().unwrap();
        assert!(index.sealed_ranges.get(&txn, &0).unwrap().is_none());
        assert_eq!(index.mutable_blocks.len(&txn).unwrap(), 1_000);
        assert_eq!(index.hash_to_height.len(&txn).unwrap(), 1_000);
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
        let mut previous_hash = ancestor.hash;
        let replacement = (31..=55)
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
            .collect();
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
        let mut previous_hash = common.hash;
        let replacement = (951..=1_110)
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
            .collect();
        let state =
            index.replace_deep_suffix(old.generation(), common, replacement, Some(1_010)).unwrap();

        assert_eq!(state.sealed_through(), Some(999));
        assert_eq!(index.read_block(state.generation(), 950).unwrap().hash, hash(950));
        assert_eq!(index.read_block(state.generation(), 951).unwrap().hash, branch_hash(951));
        assert!(index.read_block_by_hash(state.generation(), hash(951)).unwrap().is_none());
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

    fn prepared(height: u32) -> ParsedCompactBlock {
        ParsedCompactBlock {
            height,
            hash: hash(height),
            previous_hash: height.checked_sub(1).map(hash).unwrap_or([0; 32]),
            time: height,
            transactions: Vec::new(),
            sapling_additions: 0,
            orchard_additions: 0,
            ironwood_additions: 0,
        }
    }

    fn hash(height: u32) -> Digest {
        let mut hash = [0; 32];
        hash[..4].copy_from_slice(&height.to_be_bytes());
        hash
    }

    fn branch_hash(height: u32) -> Digest {
        let mut hash = hash(height);
        hash[31] = 1;
        hash
    }
}
