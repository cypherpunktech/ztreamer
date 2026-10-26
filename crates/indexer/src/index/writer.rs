//! One LMDB write transaction over the index.
//!
//! Every mutation keeps the mutable rows, the hash index, the sealed ranges, and the persisted
//! state consistent, and [`IndexWriter::commit`] is the only path that advances the generation.

use std::iter::Peekable;

use heed::{Env, RwTxn};

use super::{
    BlockId, IndexError, IndexState, RANGE_SIZE,
    range_meta::{RangeMeta, VerifyState},
    tables::Tables,
    telemetry::{WriteStage, WriteTimer, record_state_metrics},
};
use crate::{
    codec::{CompactBlockRecord, TreeSizes, encode_range_from_bytes},
    ingest::EncodedBlock,
};

/// Batch rows that are not yet in any table, in height order.
pub(crate) type BatchRows = Peekable<std::vec::IntoIter<EncodedBlock>>;

/// An open write transaction and the [`IndexState`] it will commit.
///
/// Rows change through one of three sets of methods:
///
/// - Reorgs use [`IndexWriter::put_block`] and [`IndexWriter::delete_block`], which always
///   change a block row and its hash entry together. In one transaction, do all deletes before the
///   first put: the old and new branches share the hashes of the blocks up to the common
///   ancestor, so a delete after a put can delete a canonical hash.
/// - Appends use [`IndexWriter::put_hashes`] for the whole batch first, then seal and append
///   the rows. A batch row that goes into a sealed range is never written as a mutable row.
/// - Installs use [`IndexWriter::put_new_hashes`] for the blocks of one packed range, then
///   [`IndexWriter::put_sealed_range`]. An install writes no mutable row.
///
/// Dropping a writer without [`IndexWriter::commit`] aborts the transaction.
pub(crate) struct IndexWriter<'env> {
    txn: RwTxn<'env>,
    tables: Tables,
    state: IndexState,
    timer: Option<WriteTimer>,
}

impl<'env> IndexWriter<'env> {
    /// Starts a write transaction, and rejects a write built from another generation.
    pub(crate) fn start(
        env: &'env Env,
        tables: Tables,
        base_generation: u64,
    ) -> Result<Self, IndexError> {
        let writer = Self::start_latest(env, tables)?;
        if writer.state.generation != base_generation {
            return Err(IndexError::StaleBatch { batch_generation: base_generation });
        }
        Ok(writer)
    }

    /// Starts a write transaction on the current generation. The caller must check the state.
    pub(crate) fn start_latest(env: &'env Env, tables: Tables) -> Result<Self, IndexError> {
        let txn = env.write_txn()?;
        let state = tables.metadata.get_state(&txn)?;
        Ok(Self { txn, tables, state, timer: None })
    }

    /// The state as this transaction has changed it so far.
    pub(crate) const fn state(&self) -> IndexState {
        self.state
    }

    /// Reads the block at `height` as this transaction sees it.
    pub(crate) fn read(&self, height: u32) -> Result<CompactBlockRecord, IndexError> {
        self.tables.read(&self.txn, self.state, height)
    }

    /// Times the write stages from now on. [`IndexWriter::commit`] records them.
    pub(crate) fn start_timer(&mut self) {
        self.timer = Some(WriteTimer::start());
    }

    /// Writes `record` as a mutable row, and its hash entry.
    pub(crate) fn put_block(&mut self, record: &CompactBlockRecord) -> Result<(), IndexError> {
        let encoded = record.encode()?;
        self.tables.mutable_blocks.put(&mut self.txn, record.height, encoded.as_slice())?;
        self.tables.hash_to_height.put(&mut self.txn, &record.hash, record.height)
    }

    /// Deletes the hash entry of `record`, and its mutable row if it has one. A sealed block has
    /// no mutable row.
    pub(crate) fn delete_block(&mut self, record: &CompactBlockRecord) -> Result<(), IndexError> {
        self.tables.hash_to_height.delete(&mut self.txn, &record.hash)?;
        self.tables.mutable_blocks.delete(&mut self.txn, record.height)?;
        Ok(())
    }

    /// Deletes the mutable row at `height` and its hash entry. The row must exist.
    pub(crate) fn delete_mutable_block(&mut self, height: u32) -> Result<(), IndexError> {
        let bytes = self
            .tables
            .mutable_blocks
            .get(&self.txn, height)?
            .ok_or(IndexError::Continuity { height })?;
        let old = CompactBlockRecord::decode(bytes)?;
        self.delete_block(&old)
    }

    /// Writes the hash entry of each block in an installed range. Refuses a hash that the index
    /// holds already, also a hash that occurs two times in `blocks`.
    pub(crate) fn put_new_hashes(&mut self, blocks: &[BlockId]) -> Result<(), IndexError> {
        blocks.iter().try_for_each(|block| {
            if self.tables.hash_to_height.get(&self.txn, &block.hash)?.is_some() {
                return Err(IndexError::DuplicateHash { height: block.height });
            }
            self.tables.hash_to_height.put(&mut self.txn, &block.hash, block.height)
        })
    }

    /// Writes the hash entry of every block in an appended batch.
    pub(crate) fn put_hashes(&mut self, blocks: &[EncodedBlock]) -> Result<(), IndexError> {
        blocks.iter().try_for_each(|block| {
            self.tables.hash_to_height.put(&mut self.txn, &block.hash, block.height)
        })?;
        self.with_timer(|timer| timer.lap(WriteStage::HashIndex));
        Ok(())
    }

    /// Writes the batch rows that no range took as mutable rows.
    pub(crate) fn put_mutable_blocks(&mut self, blocks: BatchRows) -> Result<(), IndexError> {
        let mut wrote = false;
        for block in blocks {
            self.tables.mutable_blocks.put(&mut self.txn, block.height, block.bytes.as_slice())?;
            self.with_timer(|timer| timer.bytes(WriteStage::MutablePut, block.bytes.len()));
            wrote = true;
        }
        self.with_timer(|timer| timer.lap_if(wrote, WriteStage::MutablePut));
        Ok(())
    }

    pub(crate) const fn set_tip(&mut self, tip: BlockId, tree_sizes: TreeSizes) {
        self.state.durable_tip = Some(tip);
        self.state.tree_sizes = tree_sizes;
    }

    /// Deletes every sealed range from `start` on, with its metadata, so that its blocks can be
    /// written again as mutable rows. A verified prefix that reached `start` ends below it.
    pub(crate) fn delete_ranges_from(&mut self, start: u32) -> Result<(), IndexError> {
        let Some(old_sealed) = self.state.sealed_through.filter(|sealed| start <= *sealed) else {
            return Ok(());
        };
        (start..=old_sealed).step_by(RANGE_SIZE as usize).try_for_each(|range| {
            self.tables.sealed_ranges.delete(&mut self.txn, range).map(drop)
        })?;
        self.tables.range_meta.delete_from(&mut self.txn, start)?;
        self.state.sealed_through = start.checked_sub(1);
        self.state.verified_through = self.state.verified_through.min(self.state.sealed_through);
        Ok(())
    }

    /// Writes a packed range from a snapshot and its metadata. Moves `sealed_through` to the
    /// range end. A [`VerifyState::Verified`] range advances `verified_through` if the range
    /// starts at [`IndexState::next_verify_start`]. The caller must check that `start` is
    /// [`IndexState::next_range_start`].
    pub(crate) fn put_sealed_range(
        &mut self,
        start: u32,
        packed: &[u8],
        meta: &RangeMeta,
    ) -> Result<(), IndexError> {
        self.tables.sealed_ranges.put(&mut self.txn, start, packed)?;
        self.tables.range_meta.put(&mut self.txn, start, meta)?;
        self.state.sealed_through = Some(meta.entry.end_height);
        if meta.state == VerifyState::Verified && self.state.next_verify_start()? == start {
            self.state.verified_through = Some(meta.entry.end_height);
        }
        Ok(())
    }

    /// Seals each range that ends at or below both `seal_through` and the tip. Call it after
    /// [`IndexWriter::set_tip`]. A range takes its mutable rows first, then rows from `blocks`;
    /// a reorg passes no `blocks`, so its ranges must be complete in the mutable rows.
    pub(crate) fn seal_ranges(
        &mut self,
        seal_through: Option<u32>,
        blocks: &mut BatchRows,
    ) -> Result<(), IndexError> {
        let Some(seal_through) = seal_through else {
            return Ok(());
        };
        let mut start = self.state.next_range_start()?;

        loop {
            let end = start.checked_add(RANGE_SIZE - 1).ok_or(IndexError::Overflow)?;
            if end > seal_through || self.state.durable_tip.is_none_or(|tip| end > tip.height) {
                return Ok(());
            }
            self.seal_range(start, end, blocks)?;
            start = end.checked_add(1).ok_or(IndexError::Overflow)?;
        }
    }

    /// Packs the range `start..=end` and deletes the mutable rows it took.
    fn seal_range(
        &mut self,
        start: u32,
        end: u32,
        blocks: &mut BatchRows,
    ) -> Result<(), IndexError> {
        // The head follower writes blocks to `mutable_blocks` before it seals them. Because of
        // this, the range can be complete in `mutable_blocks`, and the batch can start after it.
        let first_new = blocks.peek().map_or(end + 1, |block| block.height);
        let mutable_end = first_new.min(end + 1);
        let mut range =
            self.tables.mutable_blocks.read_checked(&self.txn, start..mutable_end, start)?;
        self.with_timer(|timer| timer.seal_read(range.len()));
        while range.len() < RANGE_SIZE as usize {
            let block = blocks.next().ok_or(IndexError::IncompleteRange { start })?;
            range.push(block.bytes);
        }
        let parts: Vec<_> = range.iter().map(Vec::as_slice).collect();
        let packed = encode_range_from_bytes(&parts)?;
        drop(parts);
        drop(range);
        self.with_timer(|timer| timer.lap(WriteStage::SealEncode));
        self.tables.sealed_ranges.put(&mut self.txn, start, packed.as_slice())?;
        self.tables.mutable_blocks.delete_range(&mut self.txn, start..mutable_end)?;
        self.with_timer(|timer| {
            timer.lap(WriteStage::SealPut);
            timer.bytes(WriteStage::SealPut, packed.len());
        });
        self.state.sealed_through = Some(end);
        self.join_sealed_range_to_verified_prefix(start, end);
        Ok(())
    }

    /// Moves `verified_through` to `end` if the range `start..=end`, which the index sealed from
    /// node blocks, starts directly after the verified prefix. A replica trusts its source node
    /// as much as its verified snapshot. Thus a range that it seals from the tail or the head
    /// follower joins the prefix. An index with no `verified_through`, as in the embedded mode,
    /// keeps it unset.
    fn join_sealed_range_to_verified_prefix(&mut self, start: u32, end: u32) {
        if self.state.verified_through.and_then(|through| through.checked_add(1)) == Some(start) {
            self.state.verified_through = Some(end);
        }
    }

    /// Advances the generation, writes the state, and commits. This is the only path that
    /// commits an index write.
    pub(crate) fn commit(mut self) -> Result<IndexState, IndexError> {
        self.state.generation = self.state.generation.checked_add(1).ok_or(IndexError::Overflow)?;
        self.tables.metadata.put_state(&mut self.txn, self.state)?;
        self.with_timer(|timer| timer.lap(WriteStage::StatePut));
        let Self { txn, state, timer, .. } = self;
        txn.commit()?;
        if let Some(mut timer) = timer {
            timer.lap(WriteStage::Commit);
            timer.finish();
        }
        record_state_metrics(state);
        Ok(state)
    }

    fn with_timer(&mut self, record: impl FnOnce(&mut WriteTimer)) {
        if let Some(timer) = &mut self.timer {
            record(timer);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zakura_chain::block;

    use crate::{
        index::Index,
        test_utils::{populate, record},
    };

    /// An index that holds the mutable blocks 0..=10.
    fn index_with_blocks(dir: &tempfile::TempDir) -> (Index, IndexState) {
        let index =
            Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap();
        let records: Vec<_> = (0..=10).map(|height| record(height, Vec::new())).collect();
        let state = populate(&index, &records, None);
        (index, state)
    }

    #[test]
    fn start_rejects_a_stale_generation() {
        let dir = tempfile::tempdir().unwrap();
        let (index, state) = index_with_blocks(&dir);

        let stale = state.generation() - 1;
        assert!(matches!(
            IndexWriter::start(&index.env, index.tables, stale),
            Err(IndexError::StaleBatch { batch_generation }) if batch_generation == stale
        ));
    }

    #[test]
    fn dropping_a_writer_aborts_its_transaction() {
        let dir = tempfile::tempdir().unwrap();
        let (index, state) = index_with_blocks(&dir);

        let mut writer = IndexWriter::start(&index.env, index.tables, state.generation()).unwrap();
        writer.delete_mutable_block(10).unwrap();
        writer.put_block(&record(11, Vec::new())).unwrap();
        drop(writer);

        assert_eq!(index.state().unwrap(), state);
        let txn = index.env.read_txn().unwrap();
        assert_eq!(index.tables.mutable_blocks.len(&txn).unwrap(), 11);
        assert_eq!(index.tables.hash_to_height.len(&txn).unwrap(), 11);
        drop(txn);
        index.verify_continuity().unwrap();
    }

    #[test]
    fn commit_advances_the_generation_once() {
        let dir = tempfile::tempdir().unwrap();
        let (index, state) = index_with_blocks(&dir);

        let mut writer = IndexWriter::start(&index.env, index.tables, state.generation()).unwrap();
        let next = record(11, Vec::new());
        writer.put_block(&next).unwrap();
        writer.set_tip(BlockId::new(next.height, next.hash), next.end_tree_sizes);
        let committed = writer.commit().unwrap();

        assert_eq!(committed.generation(), state.generation() + 1);
        assert_eq!(committed.durable_tip(), Some(BlockId::new(11, next.hash)));
        assert_eq!(index.state().unwrap(), committed);
        index.verify_continuity().unwrap();
    }
}
