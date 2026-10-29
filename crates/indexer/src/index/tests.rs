use super::*;
use crate::{
    index::range_meta::{VerifyState, full_meta},
    ingest::OrderedBuilder,
    manifest_entry::{BlobDigest, BlobMismatch},
    test_utils::open_index,
};
use ztreamer_chain::{
    codec::{CodecError, ProtobufBlockRecord, StoredBlock, decode_range_record, encode_range},
    test_utils::{hash, parsed, record},
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
            .create_database::<heed::types::Bytes, heed::types::Bytes>(&mut txn, Some("metadata"))
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
    assert_eq!(index.latest().unwrap().range_meta(0).unwrap(), None);
    let state = index.write(batch(&index, 0..=1_100, 1_200)).unwrap();
    assert_eq!(state.sealed_through(), Some(999));
    assert_eq!(state.verified_through(), None);
    index.put_range_meta(0, &full_meta(0)).unwrap();
    assert_eq!(index.latest().unwrap().range_meta(0).unwrap(), Some(full_meta(0)));
}

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
    let blob = index.latest().unwrap().sealed_range(1_000).unwrap();
    assert!(blob.is_some());

    assert_eq!(
        index.snapshot(generation).unwrap().range_metas_from(0).unwrap(),
        [full_meta(0), full_meta(1_000)]
    );
    assert_eq!(
        index.snapshot(generation).unwrap().range_metas_from(1).unwrap(),
        [full_meta(1_000)]
    );
    assert_eq!(index.snapshot(generation).unwrap().sealed_range(1_000).unwrap(), blob);
    assert_eq!(index.snapshot(generation).unwrap().sealed_range(2_000).unwrap(), None);
    let other = generation + 1;
    assert!(matches!(
        index.snapshot(other).and_then(|snapshot| snapshot.range_metas_from(0)),
        Err(IndexError::Generation { expected, actual }) if expected == other && actual == generation
    ));
    assert!(matches!(
        index.snapshot(other).and_then(|snapshot| snapshot.sealed_range(0)),
        Err(IndexError::Generation { .. })
    ));
}

/// One snapshot gives the row and the blob of a range. A range with no row gives `None`.
#[test]
fn a_pinned_range_read_gives_the_row_and_the_blob_of_one_generation() {
    let dir = tempfile::tempdir().unwrap();
    let (index, state) = index_with_two_range_metas(dir.path());
    let generation = state.generation();
    let blob = index.latest().unwrap().sealed_range(1_000).unwrap();
    assert!(blob.is_some());

    let snapshot = index.snapshot(generation).unwrap();
    assert_eq!(snapshot.range_meta(1_000).unwrap(), Some(full_meta(1_000)));
    assert_eq!(snapshot.sealed_range(1_000).unwrap(), blob);
    for start in [500, 2_000] {
        assert_eq!(snapshot.range_meta(start).unwrap(), None);
    }
    drop(snapshot);
    assert!(matches!(index.snapshot(generation + 1), Err(IndexError::Generation { .. })));
}

#[test]
fn range_meta_round_trips_for_a_sealed_range_and_refuses_the_rest() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("index");
    let (index, state) = index_with_two_range_metas(&path);
    assert_eq!(state.sealed_through(), Some(1_999));
    assert_eq!(index.latest().unwrap().range_meta(0).unwrap(), Some(full_meta(0)));
    assert_eq!(index.latest().unwrap().range_meta(1_000).unwrap(), Some(full_meta(1_000)));
    assert_eq!(index.latest().unwrap().range_meta(2_000).unwrap(), None);
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
    assert_eq!(index.latest().unwrap().range_meta(0).unwrap(), Some(full_meta(0)));

    drop(index);
    let reopened = open_index(&path);
    assert_eq!(reopened.latest().unwrap().range_meta(1_000).unwrap(), Some(full_meta(1_000)));
    assert_eq!(reopened.state().unwrap(), state);
}

#[test]
fn continuity_rejects_metadata_for_a_range_that_is_not_sealed() {
    let dir = tempfile::tempdir().unwrap();
    let index = open_index(dir.path());
    index.write(batch(&index, 0..=1_100, 1_200)).unwrap();

    let mut txn = index.env.write_txn().unwrap();
    index.tables.range_meta.put(&mut txn, 1_000, &full_meta(1_000)).unwrap();
    txn.commit().unwrap();
    assert!(matches!(
        index.latest().unwrap().verify_continuity(),
        Err(IndexError::RangeMeta { start: 1_000 })
    ));

    let mut txn = index.env.write_txn().unwrap();
    index.tables.range_meta.delete_from(&mut txn, 1_000).unwrap();
    txn.commit().unwrap();
    index.latest().unwrap().verify_continuity().unwrap();
}

/// A row built from the blocks of a range before a deep reorg replaced the range is refused. A
/// row for the new blocks is written.
#[test]
fn range_meta_for_the_old_blocks_of_a_replaced_range_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let index = open_index(dir.path());
    let old = index.write(batch(&index, 0..=2_100, 2_200)).unwrap();
    let common = BlockId::new(1_950, hash(1_950));
    let replacement = branch(common, 1_951..=2_110);
    let state =
        index.replace_deep_suffix(old.generation(), common, replacement, Some(2_010)).unwrap();
    assert_eq!(state.sealed_through(), Some(1_999));

    assert!(matches!(
        index.put_range_meta(1_000, &full_meta(1_000)),
        Err(IndexError::RangeEndHash { start: 1_000 })
    ));
    assert_eq!(index.latest().unwrap().range_meta(1_000).unwrap(), None);

    let mut replaced = full_meta(1_000);
    replaced.entry.end_hash = branch_hash(1_999);
    index.put_range_meta(1_000, &replaced).unwrap();
    assert_eq!(index.latest().unwrap().range_meta(1_000).unwrap(), Some(replaced));
}

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
    assert_eq!(index.latest().unwrap().range_meta(0).unwrap(), Some(full_meta(0)));
    assert_eq!(index.latest().unwrap().range_meta(1_000).unwrap(), None);
    index.latest().unwrap().verify_continuity().unwrap();

    // A reorg into the first range ends the verified prefix altogether. A range sealed with no
    // verified prefix does not start a prefix.
    let common = BlockId::new(950, hash(950));
    let replacement = branch(common, 951..=2_110);
    let state =
        index.replace_deep_suffix(state.generation(), common, replacement, Some(2_010)).unwrap();
    assert_eq!(state.verified_through(), None);
    assert_eq!(index.latest().unwrap().range_meta(0).unwrap(), None);
    index.latest().unwrap().verify_continuity().unwrap();
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

/// Gives the metadata of the range at `start`, with `state` and with the length and the
/// digest of `blob`.
fn snapshot_meta(start: u32, state: VerifyState, blob: &[u8]) -> RangeMeta {
    let mut meta = full_meta(start);
    meta.entry.end_tree_sizes = tree_sizes(start + RANGE_SIZE - 1);
    meta.entry.blob_digest = BlobDigest::hash(blob);
    meta.entry.blob_len = u64::try_from(blob.len()).unwrap();
    meta.state = state;
    meta
}

/// The blob of the range at `start`, as a snapshot gives it, and its metadata with `state`.
fn snapshot_range(start: u32, state: VerifyState) -> (Vec<u8>, RangeMeta) {
    let blob = encode_range(&range_records(start)).unwrap();
    let meta = snapshot_meta(start, state, &blob);
    (blob, meta)
}

fn install(index: &Index, start: u32, state: VerifyState) -> Result<IndexState, IndexError> {
    let (blob, meta) = snapshot_range(start, state);
    index.install_sealed_range(start, &blob, &meta)
}

#[test]
fn installs_sealed_ranges_in_order_and_reads_them_back() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("index");
    let index = open_index(&path);
    for start in [0, 1_000, 2_000] {
        install(&index, start, VerifyState::Verified).unwrap();
    }
    let state = index.state().unwrap();
    assert_eq!(state.durable_tip(), Some(BlockId::new(2_999, hash(2_999))));
    assert_eq!(state.sealed_through(), Some(2_999));
    assert_eq!(state.verified_through(), Some(2_999));
    assert_eq!(state.tree_sizes(), tree_sizes(2_999));
    assert_eq!(state.generation(), 3);
    index.latest().unwrap().verify_continuity().unwrap();

    let (blob, meta) = snapshot_range(1_000, VerifyState::Verified);
    assert_eq!(index.latest().unwrap().sealed_range(1_000).unwrap(), Some(blob));
    assert_eq!(index.latest().unwrap().sealed_range(3_000).unwrap(), None);
    assert!(matches!(
        index.latest().unwrap().sealed_range(1_500),
        Err(IndexError::RangeStart { height: 1_500 })
    ));
    assert_eq!(index.latest().unwrap().range_meta(1_000).unwrap(), Some(meta));
    for height in [0, 999, 1_000, 1_537, 2_999] {
        let block = index
            .snapshot(state.generation())
            .unwrap()
            .block::<CompactBlockRecord>(height)
            .unwrap();
        assert_eq!((block.hash, block.end_tree_sizes), (hash(height), tree_sizes(height)));
        let block = index
            .snapshot(state.generation())
            .unwrap()
            .block_by_hash::<CompactBlockRecord>(hash(height))
            .unwrap();
        assert_eq!(block.unwrap().height, height);
    }

    drop(index);
    let reopened = open_index(&path);
    assert_eq!(reopened.state().unwrap(), state);
    assert_eq!(
        reopened.latest().unwrap().range_meta(2_000).unwrap(),
        Some(snapshot_range(2_000, VerifyState::Verified).1)
    );
}

#[test]
fn install_refuses_a_range_out_of_order() {
    let dir = tempfile::tempdir().unwrap();
    let index = open_index(dir.path());
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
    assert_eq!(index.latest().unwrap().sealed_range(2_000).unwrap(), None);
}

#[test]
fn install_refuses_a_blob_that_does_not_continue_the_chain() {
    let dir = tempfile::tempdir().unwrap();
    let index = open_index(dir.path());
    let changed = |start, change: fn(&mut [CompactBlockRecord])| {
        let mut records = range_records(start);
        change(&mut records);
        encode_range(&records).unwrap()
    };
    let refused_at = |start, blob: &[u8], height| {
        let meta = snapshot_meta(start, VerifyState::Verified, blob);
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
    let mut blob = snapshot_range(1_000, VerifyState::Verified).0;
    let mut bad = range_records(1_000)[500].clone();
    let good = bad.encode().unwrap();
    bad.previous_hash = block::Hash([9; 32]);
    let at = blob.windows(good.len()).position(|bytes| bytes == good).unwrap();
    blob[at..at + good.len()].copy_from_slice(&bad.encode().unwrap());
    let meta = snapshot_meta(1_000, VerifyState::Verified, &blob);
    assert!(matches!(
        index.install_sealed_range(1_000, &blob, &meta),
        Err(IndexError::RangeBlob { height: 1_500 })
    ));
    assert_eq!(index.state().unwrap().sealed_through(), Some(999));
}

#[test]
fn install_refuses_a_block_hash_that_is_in_the_index_already() {
    let dir = tempfile::tempdir().unwrap();
    let index = open_index(dir.path());
    let state = install(&index, 0, VerifyState::Verified).unwrap();
    // The block at 1,500 gets the hash `duplicate`. The block at 1,501 links to it.
    let install_with_duplicate_hash = |duplicate| {
        let mut records = range_records(1_000);
        records[500].hash = duplicate;
        records[501].previous_hash = duplicate;
        let blob = encode_range(&records).unwrap();
        let meta = snapshot_meta(1_000, VerifyState::Verified, &blob);
        index.install_sealed_range(1_000, &blob, &meta)
    };

    // The hash of block 5 is in range 0.
    assert!(matches!(
        install_with_duplicate_hash(hash(5)),
        Err(IndexError::DuplicateHash { height: 1_500 })
    ));
    // The hash of block 1,600 occurs at 1,500 and at 1,600 in the same blob.
    assert!(matches!(
        install_with_duplicate_hash(hash(1_600)),
        Err(IndexError::DuplicateHash { height: 1_600 })
    ));
    assert_eq!(index.state().unwrap(), state);
    assert_eq!(
        index
            .snapshot(state.generation())
            .unwrap()
            .block_by_hash::<CompactBlockRecord>(hash(5))
            .unwrap()
            .unwrap()
            .height,
        5
    );
    index.latest().unwrap().verify_continuity().unwrap();
}

#[test]
fn install_refuses_a_meta_that_does_not_describe_the_blob() {
    let dir = tempfile::tempdir().unwrap();
    let index = open_index(dir.path());
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
    assert_eq!(index.latest().unwrap().range_meta(0).unwrap(), None);
    index.install_sealed_range(0, &blob, &meta).unwrap();
}

#[test]
fn install_refuses_a_blob_with_a_wrong_length_or_digest() {
    let dir = tempfile::tempdir().unwrap();
    let index = open_index(dir.path());
    let state = install(&index, 0, VerifyState::Verified).unwrap();
    let (blob, meta) = snapshot_range(1_000, VerifyState::Verified);
    let len = meta.entry.blob_len;

    let mut long = meta.clone();
    long.entry.blob_len += 1;
    assert!(matches!(
        index.install_sealed_range(1_000, &blob, &long),
        Err(IndexError::Blob { start: 1_000, mismatch: BlobMismatch::Len { expected, actual } })
            if expected == len + 1 && actual == len
    ));
    let mut other = meta.clone();
    other.entry.blob_digest = BlobDigest::hash(b"another blob");
    assert!(matches!(
        index.install_sealed_range(1_000, &blob, &other),
        Err(IndexError::Blob { start: 1_000, mismatch: BlobMismatch::Digest })
    ));
    // A changed byte does not change the length. It changes the digest.
    let mut changed = blob.clone();
    changed[blob.len() / 2] ^= 1;
    assert!(matches!(
        index.install_sealed_range(1_000, &changed, &meta),
        Err(IndexError::Blob { start: 1_000, mismatch: BlobMismatch::Digest })
    ));

    assert_eq!(index.state().unwrap(), state);
    assert_eq!(index.latest().unwrap().sealed_range(1_000).unwrap(), None);
    assert_eq!(index.latest().unwrap().range_meta(1_000).unwrap(), None);
    index.latest().unwrap().verify_continuity().unwrap();
    index.install_sealed_range(1_000, &blob, &meta).unwrap();
}

#[test]
fn install_refuses_an_index_with_mutable_rows() {
    let dir = tempfile::tempdir().unwrap();
    let index = open_index(dir.path());
    let state = index.write(batch(&index, 0..=1_100, 1_200)).unwrap();
    assert_eq!(state.sealed_through(), Some(999));
    assert!(matches!(install(&index, 1_000, VerifyState::Verified), Err(IndexError::MutableRows)));
    assert_eq!(index.state().unwrap(), state);

    // An index with mutable rows and no sealed range.
    let dir = tempfile::tempdir().unwrap();
    let index = open_index(dir.path());
    index.write(batch(&index, 0..=10, 20)).unwrap();
    assert!(matches!(install(&index, 0, VerifyState::Verified), Err(IndexError::MutableRows)));
}

/// The checked range at `start`, as a snapshot gives it, and its metadata with `state`.
fn checked_range(start: u32, state: VerifyState) -> (CheckedRange, RangeMeta) {
    let (blob, meta) = snapshot_range(start, state);
    (CheckedRange::check(blob.into(), &meta.entry).unwrap(), meta)
}

#[test]
fn installs_checked_ranges_as_install_sealed_range_does() {
    let (sealed_dir, checked_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (sealed, checked) = (open_index(sealed_dir.path()), open_index(checked_dir.path()));
    for (start, state) in [
        (0, VerifyState::Verified),
        (1_000, VerifyState::Unverified),
        (2_000, VerifyState::Verified),
    ] {
        let expected = install(&sealed, start, state).unwrap();
        let (range, meta) = checked_range(start, state);

        assert_eq!(checked.install_checked_range(&range, &meta).unwrap(), expected);
        assert_eq!(
            checked.latest().unwrap().sealed_range(start).unwrap(),
            sealed.latest().unwrap().sealed_range(start).unwrap()
        );
        assert_eq!(checked.latest().unwrap().range_meta(start).unwrap(), Some(meta));
    }
    let state = checked.state().unwrap();
    assert_eq!((state.sealed_through(), state.verified_through()), (Some(2_999), Some(999)));
    for height in [0, 1_537, 2_999] {
        let block = checked
            .snapshot(state.generation())
            .unwrap()
            .block_by_hash::<CompactBlockRecord>(hash(height))
            .unwrap();
        assert_eq!(block.unwrap().height, height);
    }
    checked.latest().unwrap().verify_continuity().unwrap();
}

/// A [`CheckedRange`] proves only the checks that do not need the index. Thus the install
/// must refuse what [`Index::install_sealed_range`] refuses for the index.
#[test]
fn install_checked_range_refuses_what_install_sealed_range_refuses() {
    let dir = tempfile::tempdir().unwrap();
    let index = open_index(dir.path());
    let (range, meta) = checked_range(1_000, VerifyState::Verified);
    assert!(matches!(
        index.install_checked_range(&range, &meta),
        Err(IndexError::RangeOrder { start: 1_000, next: 0 })
    ));
    let state = install(&index, 0, VerifyState::Verified).unwrap();
    let checked_with = |change: fn(&mut [CompactBlockRecord])| {
        let mut records = range_records(1_000);
        change(&mut records);
        let blob = encode_range(&records).unwrap();
        let meta = snapshot_meta(1_000, VerifyState::Verified, &blob);
        (CheckedRange::check(blob.into(), &meta.entry).unwrap(), meta)
    };

    // The check of the range does not know the tip. The install checks the first record
    // against the tip.
    let unlinked: fn(&mut [CompactBlockRecord]) =
        |records| records[0].previous_hash = block::Hash([9; 32]);
    let smaller_than_tip: fn(&mut [CompactBlockRecord]) =
        |records| records[0].end_tree_sizes = TreeSizes::default();
    for change in [unlinked, smaller_than_tip] {
        let (range, meta) = checked_with(change);
        assert!(matches!(
            index.install_checked_range(&range, &meta),
            Err(IndexError::RangeBlob { height: 1_000 })
        ));
    }

    // The meta must give the length and the digest that the check compared.
    let mut long = meta.clone();
    long.entry.blob_len += 1;
    assert!(matches!(
        index.install_checked_range(&range, &long),
        Err(IndexError::Blob { start: 1_000, mismatch: BlobMismatch::Len { .. } })
    ));
    let mut other = meta.clone();
    other.entry.blob_digest = BlobDigest::hash(b"another blob");
    assert!(matches!(
        index.install_checked_range(&range, &other),
        Err(IndexError::Blob { start: 1_000, mismatch: BlobMismatch::Digest })
    ));
    // The meta must describe the range.
    let mut rejected = meta.clone();
    rejected.state = VerifyState::Rejected;
    let mut end_hash = meta.clone();
    end_hash.entry.end_hash = hash(1_000);
    for meta in [rejected, end_hash] {
        assert!(matches!(
            index.install_checked_range(&range, &meta),
            Err(IndexError::RangeMeta { start: 1_000 })
        ));
    }
    // The hash of block 5 is in range 0.
    let (duplicate, duplicate_meta) = checked_with(|records| {
        records[500].hash = hash(5);
        records[501].previous_hash = hash(5);
    });
    assert!(matches!(
        index.install_checked_range(&duplicate, &duplicate_meta),
        Err(IndexError::DuplicateHash { height: 1_500 })
    ));

    assert_eq!(index.state().unwrap(), state);
    assert_eq!(index.latest().unwrap().sealed_range(1_000).unwrap(), None);
    index.latest().unwrap().verify_continuity().unwrap();
    index.install_checked_range(&range, &meta).unwrap();

    // An index with mutable rows.
    let dir = tempfile::tempdir().unwrap();
    let index = open_index(dir.path());
    index.write(batch(&index, 0..=10, 20)).unwrap();
    let (range, meta) = checked_range(0, VerifyState::Verified);
    assert!(matches!(index.install_checked_range(&range, &meta), Err(IndexError::MutableRows)));
}

#[test]
fn only_a_verified_range_after_the_verified_prefix_advances_verified_through() {
    let dir = tempfile::tempdir().unwrap();
    let index = open_index(dir.path());
    let verified = |state: IndexState| state.verified_through();
    assert_eq!(verified(install(&index, 0, VerifyState::Verified).unwrap()), Some(999));
    assert_eq!(verified(install(&index, 1_000, VerifyState::Unverified).unwrap()), Some(999));
    // The range at 1,000 is not verified. Thus the verified prefix stays at 999.
    let state = install(&index, 2_000, VerifyState::Verified).unwrap();
    assert_eq!((state.sealed_through(), verified(state)), (Some(2_999), Some(999)));

    let dir = tempfile::tempdir().unwrap();
    let index = open_index(dir.path());
    assert_eq!(verified(install(&index, 0, VerifyState::Unverified).unwrap()), None);
    index.latest().unwrap().verify_continuity().unwrap();
}

#[test]
fn a_range_sealed_after_the_verified_prefix_advances_verified_through() {
    let dir = tempfile::tempdir().unwrap();
    let index = open_index(dir.path());
    install(&index, 0, VerifyState::Verified).unwrap();
    let state = index.write(batch(&index, 1_000..=2_100, 2_200)).unwrap();
    assert_eq!((state.sealed_through(), state.verified_through()), (Some(1_999), Some(1_999)));
    assert_eq!(index.latest().unwrap().range_meta(1_000).unwrap(), None);
    index.latest().unwrap().verify_continuity().unwrap();

    // The range at 1,000 is not verified. Thus a range sealed at 2,000 is not directly after
    // the verified prefix.
    let dir = tempfile::tempdir().unwrap();
    let index = open_index(dir.path());
    install(&index, 0, VerifyState::Verified).unwrap();
    install(&index, 1_000, VerifyState::Unverified).unwrap();
    let state = index.write(batch(&index, 2_000..=3_100, 3_200)).unwrap();
    assert_eq!((state.sealed_through(), state.verified_through()), (Some(2_999), Some(999)));
}

#[test]
fn a_range_sealed_with_no_verified_prefix_keeps_verified_through_unset() {
    let dir = tempfile::tempdir().unwrap();
    let index = open_index(dir.path());
    let state = index.write(batch(&index, 0..=1_100, 1_200)).unwrap();
    assert_eq!((state.sealed_through(), state.verified_through()), (Some(999), None));

    let dir = tempfile::tempdir().unwrap();
    let index = open_index(dir.path());
    install(&index, 0, VerifyState::Unverified).unwrap();
    let state = index.write(batch(&index, 1_000..=2_100, 2_200)).unwrap();
    assert_eq!((state.sealed_through(), state.verified_through()), (Some(1_999), None));
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
        assert_eq!(
            index
                .snapshot(state.generation())
                .unwrap()
                .block::<CompactBlockRecord>(height)
                .unwrap()
                .height,
            height
        );
    }
    let txn = index.env.read_txn().unwrap();
    assert!(index.tables.mutable_blocks.get(&txn, 999).unwrap().is_none());
    drop(txn);
    index.latest().unwrap().verify_continuity().unwrap();
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
    let encoded = ztreamer_chain::codec::encode_range(&records).unwrap();
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
    index.latest().unwrap().verify_continuity().unwrap();
}

#[test]
fn reads_sealed_and_mutable_ranges_in_both_directions() {
    let dir = tempfile::tempdir().unwrap();
    let index = open_index(dir.path());
    let state = index.write(batch(&index, 0..=1_005, 1_100)).unwrap();

    assert_eq!(
        index
            .snapshot(state.generation())
            .unwrap()
            .block::<CompactBlockRecord>(537)
            .unwrap()
            .height,
        537
    );
    assert_eq!(
        index
            .snapshot(state.generation())
            .unwrap()
            .block_by_hash::<CompactBlockRecord>(hash(1_002))
            .unwrap()
            .unwrap()
            .height,
        1_002
    );

    let mut ascending = Vec::new();
    index
        .snapshot(state.generation())
        .unwrap()
        .read_blocks::<CompactBlockRecord>(998, 1_002, |block| {
            ascending.push(block.height);
            true
        })
        .unwrap();
    assert_eq!(ascending, [998, 999, 1_000, 1_001, 1_002]);

    let mut descending = Vec::new();
    index
        .snapshot(state.generation())
        .unwrap()
        .read_blocks::<CompactBlockRecord>(1_002, 998, |block| {
            descending.push(block.height);
            true
        })
        .unwrap();
    assert_eq!(descending, [1_002, 1_001, 1_000, 999, 998]);
    assert!(matches!(
        index
            .snapshot(state.generation() - 1)
            .and_then(|snapshot| snapshot.block::<CompactBlockRecord>(0)),
        Err(IndexError::Generation { .. })
    ));

    let mut latest = Vec::new();
    index
        .latest()
        .unwrap()
        .read_blocks::<CompactBlockRecord>(1_002, 998, |block| {
            latest.push(block.height);
            true
        })
        .unwrap();
    assert_eq!(latest, descending);
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
                .snapshot(old.generation())
                .unwrap()
                .read_blocks::<CompactBlockRecord>(29, 32, |block| {
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
        let state =
            index.replace_mutable_suffix(old.generation(), ancestor, replacement, None).unwrap();
        release_tx.send(()).unwrap();
        (state, reader.join().unwrap())
    });

    assert_eq!(state.durable_tip(), Some(BlockId::new(55, branch_hash(55))));
    assert_eq!(pinned_hashes, [hash(29), hash(30), hash(31), hash(32)]);
    assert!(
        index
            .snapshot(state.generation())
            .unwrap()
            .block_by_hash::<CompactBlockRecord>(hash(31))
            .unwrap()
            .is_none()
    );
    assert_eq!(
        index.snapshot(state.generation()).unwrap().block::<CompactBlockRecord>(31).unwrap().hash,
        branch_hash(31)
    );
    index.latest().unwrap().verify_continuity().unwrap();
}

#[test]
fn mutable_reorg_seals_a_range_that_it_completes() {
    let dir = tempfile::tempdir().unwrap();
    let index = open_index(dir.path());
    let old = index.write(batch(&index, 0..=1_050, 1_060)).unwrap();
    assert_eq!(old.sealed_through(), None);

    let ancestor = BlockId::new(1_040, hash(1_040));
    let replacement = branch(ancestor, 1_041..=1_045);
    let state =
        index.replace_mutable_suffix(old.generation(), ancestor, replacement, Some(999)).unwrap();

    assert_eq!(state.sealed_through(), Some(999));
    assert_eq!(state.durable_tip(), Some(BlockId::new(1_045, branch_hash(1_045))));
    let txn = index.env.read_txn().unwrap();
    assert_eq!(index.tables.mutable_blocks.len(&txn).unwrap(), 46);
    drop(txn);
    assert_eq!(
        index.snapshot(state.generation()).unwrap().block::<CompactBlockRecord>(537).unwrap().hash,
        hash(537)
    );
    index.latest().unwrap().verify_continuity().unwrap();
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
    assert_eq!(
        index.snapshot(state.generation()).unwrap().block::<CompactBlockRecord>(950).unwrap().hash,
        hash(950)
    );
    assert_eq!(
        index.snapshot(state.generation()).unwrap().block::<CompactBlockRecord>(951).unwrap().hash,
        branch_hash(951)
    );
    assert!(
        index
            .snapshot(state.generation())
            .unwrap()
            .block_by_hash::<CompactBlockRecord>(hash(951))
            .unwrap()
            .is_none()
    );
    index.latest().unwrap().verify_continuity().unwrap();
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
        let block = index
            .snapshot(state.generation())
            .unwrap()
            .block_by_hash::<CompactBlockRecord>(hash(height))
            .unwrap();
        assert_eq!(block.unwrap().height, height);
    }
    assert!(
        index
            .snapshot(state.generation())
            .unwrap()
            .block_by_hash::<CompactBlockRecord>(hash(1_000))
            .unwrap()
            .is_none()
    );
    assert_eq!(
        index
            .snapshot(state.generation())
            .unwrap()
            .block::<CompactBlockRecord>(1_000)
            .unwrap()
            .hash,
        branch_hash(1_000)
    );
    index.latest().unwrap().verify_continuity().unwrap();
}

fn batch(index: &Index, heights: std::ops::RangeInclusive<u32>, source_tip: u32) -> WriteBatch {
    let mut builder = OrderedBuilder::new(index.state().unwrap(), 10 * 1024 * 1024).unwrap();
    for height in heights {
        builder.push(parsed(record(height, Vec::new()))).unwrap();
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
fn branch(ancestor: BlockId, heights: std::ops::RangeInclusive<u32>) -> Vec<CompactBlockRecord> {
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
