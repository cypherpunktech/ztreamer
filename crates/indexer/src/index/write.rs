//! Block writes, range sealing, and the coverage check.

use std::time::Instant;

use super::{
    BlockId, Index, IndexError, IndexState, RANGE_SIZE,
    tables::{Tables, range_start},
    telemetry::WriteStage,
    writer::IndexWriter,
};
use crate::{
    codec::{CompactBlockRecord, decode_range_record},
    ingest::{EncodedBatch, WriteBatch},
};

impl Index {
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
}

#[cfg(test)]
mod tests;
