//! Deterministic synthetic compact blocks shared by tests and benchmarks.
//!
//! Compiled for this crate's own tests and for other crates that enable the `test-utils`
//! feature in their `[dev-dependencies]`.

use crate::{
    codec::{CompactBlockRecord, TreeSizes},
    index::{Index, IndexState},
    ingest::OrderedBuilder,
    parser::parse_block,
    types::{CompactTransaction, ParsedCompactBlock, RawIndexBlock},
};
use std::sync::Arc;

use zakura_chain::{
    block::{self, Block},
    fmt::HexDebug,
    parameters::GENESIS_PREVIOUS_BLOCK_HASH,
    serialization::ZcashDeserialize as _,
    transaction::{self, LockTime, Transaction},
    transparent,
};
use zakura_test::vectors::{
    BLOCK_MAINNET_1_BYTES, BLOCK_MAINNET_949496_BYTES, BLOCK_MAINNET_1687121_BYTES,
    BLOCK_MAINNET_GENESIS_BYTES, BLOCK_TESTNET_1842421_BYTES,
};

/// A deterministic block hash: the big-endian height in the first four bytes, zeros after.
pub fn hash(height: u32) -> block::Hash {
    let mut hash = [0; 32];
    hash[..4].copy_from_slice(&height.to_be_bytes());
    block::Hash(hash)
}

/// The shielded transactions from three real mainnet and testnet block vectors.
pub fn shielded_transactions() -> Vec<CompactTransaction> {
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

/// A record at `height` that chains onto [`hash`] of the previous height.
pub fn record(height: u32, transactions: Vec<CompactTransaction>) -> CompactBlockRecord {
    CompactBlockRecord {
        height,
        hash: hash(height),
        previous_hash: height.checked_sub(1).map(hash).unwrap_or(GENESIS_PREVIOUS_BLOCK_HASH),
        time: height,
        transactions,
        end_tree_sizes: TreeSizes::default(),
    }
}

/// Converts a record into the parser output that produces it, with matching tree additions.
pub fn parsed(record: CompactBlockRecord) -> ParsedCompactBlock {
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

/// `count` records with a mix of empty, small and heavy blocks and running tree sizes.
///
/// Deliberately mixed sizes, not a claim about mainnet's statistical distribution.
pub fn mixed_records(count: u32) -> Vec<CompactBlockRecord> {
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
                tx.txid.0[..4].copy_from_slice(&height.to_be_bytes());
                tx.txid.0[4..12].copy_from_slice(&(index as u64).to_be_bytes());
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

/// Writes `records` into `index` in batches and returns the resulting state.
pub fn populate(
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

/// An empty block at `height` that chains onto [`hash`] of the previous height.
pub fn prepared(height: u32) -> ParsedCompactBlock {
    ParsedCompactBlock {
        height,
        hash: hash(height),
        previous_hash: height.checked_sub(1).map(hash).unwrap_or(GENESIS_PREVIOUS_BLOCK_HASH),
        time: height,
        transactions: Vec::new(),
        sapling_additions: 0,
        orchard_additions: 0,
        ironwood_additions: 0,
    }
}

/// A minimal raw block at `height`: a 140-byte header prefix that chains onto [`hash`] of the
/// previous height, plus one empty transaction.
pub fn raw_block(height: u32) -> RawIndexBlock {
    let mut bytes = vec![0; 140];
    bytes[4..36]
        .copy_from_slice(&height.checked_sub(1).map(hash).unwrap_or(GENESIS_PREVIOUS_BLOCK_HASH).0);
    bytes[100..104].copy_from_slice(&height.to_le_bytes());
    bytes.extend_from_slice(&[0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
    RawIndexBlock {
        height: block::Height(height),
        hash: hash(height),
        bytes,
        txids: vec![transaction::Hash(hash(height).0)],
    }
}

/// Extends `chain` with deserializable blocks until its tip is at `tip`.
///
/// An empty `chain` starts with the mainnet genesis block. Each new block has the header of
/// mainnet block 1 with the hash of the block before it as parent, and one version 1 coinbase
/// transaction for its height. `branch` goes into the header nonce: two chains that are
/// extended from the same blocks with different values have different hashes from the first new
/// block. Thus a test makes a reorg from a prefix of a chain.
pub fn grow_chain(chain: &mut Vec<Block>, tip: u32, branch: u32) {
    let template = Block::zcash_deserialize(BLOCK_MAINNET_1_BYTES.as_slice()).unwrap();
    if chain.is_empty() {
        chain.push(Block::zcash_deserialize(BLOCK_MAINNET_GENESIS_BYTES.as_slice()).unwrap());
    }
    while let Some(parent) = chain.last()
        && parent.coinbase_height().unwrap().0 < tip
    {
        let height = block::Height(parent.coinbase_height().unwrap().0 + 1);
        let mut header = *template.header;
        header.previous_block_hash = parent.hash();
        header.nonce = HexDebug([0; 32]);
        header.nonce.0[..4].copy_from_slice(&branch.to_be_bytes());
        let coinbase = Transaction::V1 {
            inputs: vec![transparent::Input::Coinbase {
                height,
                data: b"ztreamer".to_vec(),
                sequence: u32::MAX,
            }],
            outputs: Vec::new(),
            lock_time: LockTime::unlocked(),
        };
        chain.push(Block { header: Arc::new(header), transactions: vec![Arc::new(coinbase)] });
    }
}
