//! The replacement of the mutable suffix and of sealed ranges after a reorg.

use super::{BlockId, Index, IndexError, IndexState, tables::range_start, writer::IndexWriter};
use crate::codec::CompactBlockRecord;

impl Index {
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
    use crate::{
        index::{range_meta::full_meta, test_support::*, *},
        test_utils::{hash, open_index},
    };

    #[test]
    fn deep_reorg_drops_the_metadata_of_rebuilt_ranges_and_they_rejoin_the_verified_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let index = open_index(dir.path());
        let old = index.write(batch(&index, 0..=2_100, 2_200)).unwrap();
        assert_eq!(old.sealed_through(), Some(1_999));
        index.put_range_meta(0, &full_meta(0)).unwrap();
        index.put_range_meta(1_000, &full_meta(1_000)).unwrap();
        let old = IndexState { verified_through: Some(1_999), ..old };
        let mut txn = index.env.write_txn().unwrap();
        index.tables.metadata.put_state(&mut txn, old).unwrap();
        txn.commit().unwrap();
        assert_eq!(index.state().unwrap().verified_through(), Some(1_999));

        // The reorg rebuilds the range 1,000..=1,999: its metadata goes, and the first range's
        // stays. The rebuilt range starts directly after the verified prefix, which ends with the
        // first range. Thus the rebuilt range joins the prefix again.
        let common = BlockId::new(1_950, hash(1_950));
        let replacement = branch(common, 1_951..=2_110);
        let state =
            index.replace_deep_suffix(old.generation(), common, replacement, Some(2_010)).unwrap();

        assert_eq!(state.sealed_through(), Some(1_999));
        assert_eq!(state.verified_through(), Some(1_999));
        assert_eq!(index.range_meta(0).unwrap(), Some(full_meta(0)));
        assert_eq!(index.range_meta(1_000).unwrap(), None);
        index.verify_continuity().unwrap();

        // A reorg into the first range ends the verified prefix altogether. A range sealed with no
        // verified prefix does not start a prefix.
        let common = BlockId::new(950, hash(950));
        let replacement = branch(common, 951..=2_110);
        let state = index
            .replace_deep_suffix(state.generation(), common, replacement, Some(2_010))
            .unwrap();
        assert_eq!(state.verified_through(), None);
        assert_eq!(index.range_meta(0).unwrap(), None);
        index.verify_continuity().unwrap();
    }

    #[test]
    fn atomically_replaces_a_mutable_suffix() {
        let dir = tempfile::tempdir().unwrap();
        let index = open_index(dir.path());
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
        let index = open_index(dir.path());
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
        let index = open_index(dir.path());
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
        let index = open_index(dir.path());
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
}
