use super::*;
use zakura_chain::transaction;

use crate::test_utils::hash;
use crate::types::{CompactSaplingOutput, CompactShieldedAction};

/// Each row: the operation, its two operands and the expected result.
#[test]
fn tree_sizes_add_and_subtract_pool_by_pool() {
    type Operation = fn(TreeSizes, TreeSizes) -> Option<TreeSizes>;
    let sizes = |sapling, orchard, ironwood| TreeSizes { sapling, orchard, ironwood };
    let (add, sub): (Operation, Operation) = (TreeSizes::checked_add, TreeSizes::checked_sub);
    let rows = [
        ("subtract", sub, sizes(5, 3, 1), sizes(2, 3, 0), Some(sizes(3, 0, 1))),
        (
            "add back the subtracted sizes",
            add,
            sizes(3, 0, 1),
            sizes(2, 3, 0),
            Some(sizes(5, 3, 1)),
        ),
        ("subtract a larger pool", sub, sizes(2, 3, 0), sizes(5, 3, 1), None),
        ("add", add, sizes(5, 6, u32::MAX - 1), sizes(1, 2, 1), Some(sizes(6, 8, u32::MAX))),
        ("add with overflow", add, sizes(5, 6, u32::MAX - 1), sizes(1, 2, 2), None),
    ];
    for (name, operation, left, right, expected) in rows {
        assert_eq!(operation(left, right), expected, "{name}");
    }
}

#[test]
fn rejects_transactions_without_shielded_data() {
    let record = CompactBlockRecord {
        height: 0,
        hash: block::Hash([0; 32]),
        previous_hash: block::Hash([0; 32]),
        time: 0,
        end_tree_sizes: TreeSizes::default(),
        transactions: vec![CompactTransaction {
            index: 0,
            txid: transaction::Hash([0; 32]),
            sapling_spends: vec![],
            sapling_outputs: vec![],
            orchard_actions: vec![],
            ironwood_actions: vec![],
        }],
    };
    assert_eq!(record.encode(), Err(CodecError::InvalidRecord));
}

#[test]
fn size_bound_covers_header_extremes_and_large_single_pool_transactions() {
    for count in [0, 1, 127, 128, 1024] {
        for pool in 0..4 {
            let tx = CompactTransaction {
                index: u64::MAX,
                txid: transaction::Hash([0; 32]),
                sapling_spends: if pool == 0 { vec![[0; 32]; count] } else { vec![] },
                sapling_outputs: if pool == 1 {
                    vec![
                        CompactSaplingOutput {
                            cmu: [0; 32],
                            ephemeral_key: [0; 32],
                            ciphertext: [0; 52],
                        };
                        count
                    ]
                } else {
                    vec![]
                },
                orchard_actions: if pool == 2 { vec![action(0); count] } else { vec![] },
                ironwood_actions: if pool == 3 { vec![action(0); count] } else { vec![] },
            };
            let record = CompactBlockRecord {
                height: u32::MAX,
                hash: block::Hash([0; 32]),
                previous_hash: block::Hash([0; 32]),
                time: u32::MAX,
                transactions: if count == 0 { vec![] } else { vec![tx] },
                end_tree_sizes: TreeSizes {
                    sapling: u32::MAX,
                    orchard: u32::MAX,
                    ironwood: u32::MAX,
                },
            };
            let bytes = record.encode().unwrap();
            assert!(
                bytes.len()
                    <= CompactBlockRecord::encoded_size_bound(&record.transactions).unwrap()
            );
            assert_eq!(CompactBlockRecord::decode(&bytes).unwrap(), record);
        }
    }
}

#[test]
fn block_and_range_round_trip() {
    let transaction = CompactTransaction {
        index: 2,
        txid: transaction::Hash([3; 32]),
        sapling_spends: vec![[4; 32]],
        sapling_outputs: vec![CompactSaplingOutput {
            cmu: [5; 32],
            ephemeral_key: [6; 32],
            ciphertext: [7; 52],
        }],
        orchard_actions: vec![action(8)],
        ironwood_actions: vec![action(9)],
    };
    let first = CompactBlockRecord {
        height: 0,
        hash: block::Hash([1; 32]),
        previous_hash: block::Hash([0; 32]),
        time: 10,
        transactions: vec![transaction],
        end_tree_sizes: TreeSizes { sapling: 1, orchard: 1, ironwood: 1 },
    };
    assert_eq!(first.additions(), TreeSizes { sapling: 1, orchard: 1, ironwood: 1 });
    let encoded_first = first.encode().unwrap();
    assert_eq!(
        EncodedBlockRecord::decode(&encoded_first).unwrap().block.encode_to_vec(),
        first.to_proto().encode_to_vec()
    );
    for len in 0..encoded_first.len() {
        assert!(CompactBlockRecord::decode(&encoded_first[..len]).is_err());
        assert!(EncodedBlockRecord::decode(&encoded_first[..len]).is_err());
    }
    let mut mismatched = encoded_first.clone();
    mismatched[1] ^= 1;
    assert_eq!(CompactBlockRecord::decode(&mismatched), Err(CodecError::InvalidRecord));
    assert!(
        encoded_first.len() <= CompactBlockRecord::encoded_size_bound(&first.transactions).unwrap()
    );
    assert_eq!(CompactBlockRecord::decode(&encoded_first).unwrap(), first);
    assert_eq!(
        CompactBlockRecord::decode(&[encoded_first, vec![0]].concat()),
        Err(CodecError::InvalidRecord)
    );
    assert_eq!(CompactBlockRecord::decode(&vec![0; MAX_RECORD_BYTES + 1]), Err(CodecError::Length));
    assert_eq!(CompactBlockRecord::decode(&[0xff; 81]), Err(CodecError::InvalidRecord));

    let mut records = vec![first];
    for height in 1..RANGE_SIZE {
        let previous_hash = records.last().unwrap().hash;
        records.push(CompactBlockRecord {
            height,
            hash: hash(height),
            previous_hash,
            time: height,
            transactions: Vec::new(),
            end_tree_sizes: TreeSizes::default(),
        });
    }
    let encoded = encode_range(&records).unwrap();
    let range = RangeDecoder::new(&encoded).unwrap();
    for (index, expected) in records.iter().enumerate() {
        let encoded = range.record_as::<EncodedBlockRecord>(index).unwrap();
        assert_eq!(encoded.height, expected.height);
        assert_eq!(encoded.hash, expected.hash);
        assert_eq!(encoded.previous_hash, expected.previous_hash);
        let actual = encoded.block.into_decoded().unwrap();
        let expected = expected.to_proto();
        assert_eq!(actual, expected);
    }
    assert_eq!(decode_range_record(&encoded, 0).unwrap(), records[0]);
    assert_eq!(decode_range_record(&encoded, 537).unwrap(), records[537]);
    assert_eq!(decode_range_record(&encoded, 999).unwrap(), records[999]);

    // Stored record bytes give the same range.
    let stored: Vec<_> = records.iter().map(|r| r.encode().unwrap()).collect();
    let mut parts: Vec<_> = stored.iter().map(Vec::as_slice).collect();
    assert_eq!(encode_range_from_bytes(&parts).unwrap(), encoded);

    // Heights out of order.
    parts.swap(3, 4);
    assert_eq!(encode_range_from_bytes(&parts), Err(CodecError::InvalidRange));
    records.swap(3, 4);
    assert_eq!(encode_range(&records), Err(CodecError::InvalidRange));
    records.swap(3, 4);

    // Heights in order, but block 500 does not point to the hash of block 499.
    records[500].previous_hash = block::Hash([0xee; 32]);
    assert_eq!(encode_range(&records), Err(CodecError::InvalidRange));
    let stored: Vec<_> = records.iter().map(|r| r.encode().unwrap()).collect();
    let parts: Vec<_> = stored.iter().map(Vec::as_slice).collect();
    assert_eq!(encode_range_from_bytes(&parts), Err(CodecError::InvalidRange));
}

#[test]
fn tree_sizes_total_sums_the_pools_without_overflow() {
    let sizes = TreeSizes { sapling: u32::MAX, orchard: u32::MAX, ironwood: 1 };

    assert_eq!(sizes.total(), 2 * u64::from(u32::MAX) + 1);
    assert!(sizes.follows(TreeSizes { ironwood: 1, ..TreeSizes::default() }));
}

fn action(byte: u8) -> CompactShieldedAction {
    CompactShieldedAction {
        nullifier: [byte; 32],
        commitment: [byte; 32],
        ephemeral_key: [byte; 32],
        ciphertext: [byte; 52],
    }
}
