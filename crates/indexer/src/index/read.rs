//! Reads of the index: `range_meta` rows, sealed ranges, blocks and block ranges.

use heed::RoTxn;
use zakura_chain::block;

use super::{
    Index, IndexError, IndexState, RANGE_SIZE, SealedRangeWithMeta, range_meta::RangeMeta,
    tables::range_start,
};
use crate::codec::{CompactBlockRecord, StoredBlock};

impl Index {
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
}

#[cfg(test)]
mod tests {
    use crate::{
        index::{range_meta::full_meta, test_support::*, *},
        test_utils::{hash, open_index},
    };

    /// An index at `path` with the blocks 0..=2,100, the ranges at 0 and 1,000 sealed, and a
    /// metadata row for each of the two ranges. Returns the state after the block write.
    fn index_with_two_range_metas(path: &Path) -> (Index, IndexState) {
        let index = open_index(path);
        let state = index.write(batch(&index, 0..=2_100, 2_200)).unwrap();
        index.put_range_meta(0, &full_meta(0)).unwrap();
        index.put_range_meta(1_000, &full_meta(1_000)).unwrap();
        (index, state)
    }

    /// The pinned reads refuse another generation. The row list starts at the given height.
    #[test]
    fn pinned_range_reads_check_the_generation() {
        let dir = tempfile::tempdir().unwrap();
        let (index, state) = index_with_two_range_metas(dir.path());
        let generation = state.generation();

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

    /// One pinned read gives the row and the blob of a range. A range with no row gives `None`.
    #[test]
    fn a_pinned_range_read_gives_the_row_and_the_blob_of_one_generation() {
        let dir = tempfile::tempdir().unwrap();
        let (index, state) = index_with_two_range_metas(dir.path());
        let generation = state.generation();

        let range = index.read_sealed_range_with_meta_at(generation, 1_000).unwrap().unwrap();
        assert_eq!(range.meta, full_meta(1_000));
        assert_eq!(Some(range.blob), index.read_sealed_range_bytes(1_000).unwrap());
        for start in [500, 2_000] {
            assert_eq!(index.read_sealed_range_with_meta_at(generation, start).unwrap(), None);
        }
        assert!(matches!(
            index.read_sealed_range_with_meta_at(generation + 1, 0),
            Err(IndexError::Generation { .. })
        ));
    }

    #[test]
    fn range_meta_round_trips_for_a_sealed_range_and_refuses_the_rest() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index");
        let (index, state) = index_with_two_range_metas(&path);
        assert_eq!(state.sealed_through(), Some(1_999));
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
        let reopened = open_index(&path);
        assert_eq!(reopened.range_meta(1_000).unwrap(), Some(full_meta(1_000)));
        assert_eq!(reopened.state().unwrap(), state);
    }

    #[test]
    fn reads_sealed_and_mutable_ranges_in_both_directions() {
        let dir = tempfile::tempdir().unwrap();
        let index = open_index(dir.path());
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
}
