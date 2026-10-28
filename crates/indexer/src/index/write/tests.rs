use crate::{
    codec::{CompactBlockRecord, ProtobufBlockRecord, StoredBlock, decode_range_record},
    index::{range_meta::full_meta, test_support::*, *},
    ingest::{OrderedBuilder, WriteBatch},
    test_utils::{hash, open_index, parsed, record},
};

#[test]
fn continuity_rejects_metadata_for_a_range_that_is_not_sealed() {
    let dir = tempfile::tempdir().unwrap();
    let index = open_index(dir.path());
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
fn atomically_writes_restarts_and_packs_sealed_ranges() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("index");
    let index = open_index(&path);
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
    let reopened = open_index(&path);
    assert_eq!(reopened.state().unwrap(), state);
}

#[test]
fn seals_a_range_that_is_complete_in_mutable_rows() {
    let dir = tempfile::tempdir().unwrap();
    let index = open_index(dir.path());
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
    let index = open_index(dir.path());
    // This write seals no range.
    let stages = write_stages(&index, batch(&index, 0..=10, 20));
    assert!(stages.iter().all(|stage| !stage.starts_with("seal_")));
    assert!(stages.contains(&"mutable_put".to_owned()));
    assert!(stages.contains(&"state_put".to_owned()));

    // This write seals a range from its batch only. It reads and writes no mutable rows.
    let dir = tempfile::tempdir().unwrap();
    let index = open_index(dir.path());
    let stages = write_stages(&index, batch(&index, 0..=999, 1_100));
    assert!(stages.contains(&"seal_encode".to_owned()));
    assert!(stages.contains(&"seal_put".to_owned()));
    assert!(!stages.contains(&"seal_read".to_owned()));
    assert!(!stages.contains(&"mutable_put".to_owned()));

    // This write seals a range from mutable rows and batch rows, then writes the rest of the
    // batch as mutable rows.
    let dir = tempfile::tempdir().unwrap();
    let index = open_index(dir.path());
    index.write(batch(&index, 0..=500, 600)).unwrap();
    let stages = write_stages(&index, batch(&index, 501..=1_050, 1_110));
    for stage in ["seal_read", "seal_encode", "seal_put", "mutable_put"] {
        assert!(stages.contains(&stage.to_owned()), "{stage} is missing from {stages:?}");
    }
}

#[test]
fn seal_fails_for_a_damaged_mutable_row() {
    let dir = tempfile::tempdir().unwrap();
    let index = open_index(dir.path());
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
    let index = open_index(dir.path());
    index.write(batch(&index, 0..=10, 20)).unwrap();

    // Add a transaction without shielded data to a row. The row still decodes, but
    // `CompactBlockRecord::encode` does not accept it, so the seal must not accept it.
    let mut txn = index.env.write_txn().unwrap();
    let row = index.tables.mutable_blocks.get(&txn, 5).unwrap().unwrap().to_vec();
    let mut block = ProtobufBlockRecord::decode(&row).unwrap().0;
    block.vtx.push(ztreamer_protocol::proto::CompactTx { txid: vec![7; 32], ..Default::default() });
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
    let index = open_index(&path);
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
    let index = open_index(dir.path());
    let mut builder = OrderedBuilder::new(IndexState::default(), 10 * 1024 * 1024).unwrap();
    builder.push(parsed(record(0, Vec::new()))).unwrap();

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
        builder.push(parsed(record(height, Vec::new()))).unwrap();
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
        builder.push(parsed(record(height, Vec::new()))).unwrap();
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
    let index = open_index(dir.path());
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
