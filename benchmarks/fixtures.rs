//! Deterministic synthetic compact blocks shared by the benchmark targets.
#![allow(dead_code)]

use zakura_chain::{block::Block, serialization::ZcashDeserialize as _};
use zakura_test::vectors::{
    BLOCK_MAINNET_949496_BYTES, BLOCK_MAINNET_1687121_BYTES, BLOCK_TESTNET_1842421_BYTES,
};
use ztreamer_indexer::{
    Digest,
    codec::{CompactBlockRecord, TreeSizes},
    index::{Index, IndexState},
    ingest::OrderedBuilder,
    parser::parse_block,
    types::{CompactTransaction, ParsedCompactBlock, RawIndexBlock},
};

pub(crate) fn hash(height: u32) -> Digest {
    let mut hash = [0; 32];
    hash[..4].copy_from_slice(&height.to_be_bytes());
    hash
}

pub(crate) fn shielded_transactions() -> Vec<CompactTransaction> {
    let mut transactions = Vec::new();
    for bytes in
        [&*BLOCK_MAINNET_949496_BYTES, &*BLOCK_MAINNET_1687121_BYTES, &*BLOCK_TESTNET_1842421_BYTES]
    {
        let block = Block::zcash_deserialize(bytes.as_slice()).unwrap();
        transactions.extend(
            parse_block(&RawIndexBlock {
                height: block.coinbase_height().unwrap(),
                hash: block.hash(),
                bytes: bytes.to_vec(),
                txids: block.transactions.iter().map(|tx| tx.hash()).collect(),
            })
            .unwrap()
            .transactions,
        );
    }
    transactions
}

pub(crate) fn record(height: u32, transactions: Vec<CompactTransaction>) -> CompactBlockRecord {
    CompactBlockRecord {
        height,
        hash: hash(height),
        previous_hash: height.checked_sub(1).map(hash).unwrap_or_default(),
        time: height,
        transactions,
        end_tree_sizes: TreeSizes::default(),
    }
}

pub(crate) fn parsed(record: CompactBlockRecord) -> ParsedCompactBlock {
    ParsedCompactBlock {
        height: record.height,
        hash: record.hash,
        previous_hash: record.previous_hash,
        time: record.time,
        sapling_additions: record
            .transactions
            .iter()
            .map(|tx| tx.sapling_outputs.len() as u32)
            .sum(),
        orchard_additions: record
            .transactions
            .iter()
            .map(|tx| tx.orchard_actions.len() as u32)
            .sum(),
        ironwood_additions: record
            .transactions
            .iter()
            .map(|tx| tx.ironwood_actions.len() as u32)
            .sum(),
        transactions: record.transactions,
    }
}

// Deliberately mixed sizes, not a claim about mainnet's statistical distribution.
pub(crate) fn mixed_records(count: u32) -> Vec<CompactBlockRecord> {
    let shielded = shielded_transactions();
    let mut sizes = TreeSizes::default();
    (0..count)
        .map(|height| {
            let copies = match height % 10 {
                0..=4 => 0,
                5..=8 => 1,
                _ => 32,
            };
            let mut transactions: Vec<_> =
                (0..copies).flat_map(|_| shielded.iter().cloned()).collect();
            for (index, tx) in transactions.iter_mut().enumerate() {
                tx.index = index as u64;
                tx.txid[..4].copy_from_slice(&height.to_be_bytes());
                tx.txid[4..12].copy_from_slice(&(index as u64).to_be_bytes());
                sizes.sapling += tx.sapling_outputs.len() as u32;
                sizes.orchard += tx.orchard_actions.len() as u32;
                sizes.ironwood += tx.ironwood_actions.len() as u32;
            }
            let mut record = record(height, transactions);
            record.end_tree_sizes = sizes;
            record
        })
        .collect()
}

pub(crate) fn populate(
    index: &Index,
    records: &[CompactBlockRecord],
    seal_through: Option<u32>,
) -> IndexState {
    let mut state = index.state().unwrap();
    let tip = records.last().unwrap().height;
    let mut builder = OrderedBuilder::new(state, 256 * 1024 * 1024).unwrap();
    for record in records {
        builder.push(parsed(record.clone())).unwrap();
    }
    while let Some(batch) = builder.build_batch(Some(tip), seal_through, 16 * 1024 * 1024).unwrap()
    {
        state = index.write(batch).unwrap();
    }
    assert_eq!(state.durable_tip().unwrap().height, tip);
    state
}
