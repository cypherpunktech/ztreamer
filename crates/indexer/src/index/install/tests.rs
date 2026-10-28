use crate::{
    codec::{CompactBlockRecord, encode_range},
    index::{
        range_meta::{BlobDigest, VerifyState, full_meta},
        test_support::*,
        *,
    },
    test_utils::{hash, open_index, record},
};

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
    let reopened = open_index(&path);
    assert_eq!(reopened.state().unwrap(), state);
    assert_eq!(
        reopened.range_meta(2_000).unwrap(),
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
    assert_eq!(index.read_sealed_range_bytes(2_000).unwrap(), None);
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
    assert_eq!(index.read_block_by_hash(state.generation(), hash(5)).unwrap().unwrap().height, 5);
    index.verify_continuity().unwrap();
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
    assert_eq!(index.range_meta(0).unwrap(), None);
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
    assert_eq!(index.read_sealed_range_bytes(1_000).unwrap(), None);
    assert_eq!(index.range_meta(1_000).unwrap(), None);
    index.verify_continuity().unwrap();
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
            checked.read_sealed_range_bytes(start).unwrap(),
            sealed.read_sealed_range_bytes(start).unwrap()
        );
        assert_eq!(checked.range_meta(start).unwrap(), Some(meta));
    }
    let state = checked.state().unwrap();
    assert_eq!((state.sealed_through(), state.verified_through()), (Some(2_999), Some(999)));
    for height in [0, 1_537, 2_999] {
        let block = checked.read_block_by_hash(state.generation(), hash(height)).unwrap();
        assert_eq!(block.unwrap().height, height);
    }
    checked.verify_continuity().unwrap();
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
    assert_eq!(index.read_sealed_range_bytes(1_000).unwrap(), None);
    index.verify_continuity().unwrap();
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
    index.verify_continuity().unwrap();
}

#[test]
fn a_range_sealed_after_the_verified_prefix_advances_verified_through() {
    let dir = tempfile::tempdir().unwrap();
    let index = open_index(dir.path());
    install(&index, 0, VerifyState::Verified).unwrap();
    let state = index.write(batch(&index, 1_000..=2_100, 2_200)).unwrap();
    assert_eq!((state.sealed_through(), state.verified_through()), (Some(1_999), Some(1_999)));
    assert_eq!(index.range_meta(1_000).unwrap(), None);
    index.verify_continuity().unwrap();

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
