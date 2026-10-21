// Disabled due to warnings in criterion macros
#![allow(missing_docs)]

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use zakura_chain::{
    block::Block,
    serialization::{ZcashDeserialize as _, ZcashSerialize as _},
};
use zakura_test::vectors::{
    BLOCK_MAINNET_396_BYTES, BLOCK_MAINNET_347500_BYTES, BLOCK_MAINNET_419200_BYTES,
    BLOCK_MAINNET_949496_BYTES, BLOCK_MAINNET_1687106_BYTES, BLOCK_MAINNET_1687121_BYTES,
    BLOCK_TESTNET_1842421_BYTES,
};
use ztreamer_indexer::parser::{RawIndexBlock, parse_block};

fn corpus() -> Vec<(&'static str, RawIndexBlock)> {
    let mut corpus: Vec<_> = [
        ("sprout-joinsplit", &*BLOCK_MAINNET_396_BYTES),
        ("overwinter", &*BLOCK_MAINNET_347500_BYTES),
        ("sapling", &*BLOCK_MAINNET_419200_BYTES),
        ("shielded-coinbase", &*BLOCK_MAINNET_949496_BYTES),
        ("nu5-v5", &*BLOCK_MAINNET_1687106_BYTES),
        ("orchard", &*BLOCK_MAINNET_1687121_BYTES),
        ("v6-ironwood", &*BLOCK_TESTNET_1842421_BYTES),
    ]
    .into_iter()
    .map(|(name, encoded_block)| {
        let bytes = encoded_block.to_vec();
        let block = Block::zcash_deserialize(bytes.as_slice()).unwrap();
        let raw = RawIndexBlock {
            height: block.coinbase_height().unwrap(),
            hash: block.hash(),
            bytes,
            txids: block.transactions.iter().map(|transaction| transaction.hash()).collect(),
        };
        (name, raw)
    })
    .collect();
    // Parser stress inputs, not consensus-valid blocks: repeat a real transaction.
    for (name, count) in [("synthetic-64-tx", 64), ("synthetic-1024-tx", 1024)] {
        let mut block = Block::zcash_deserialize(BLOCK_MAINNET_1687121_BYTES.as_slice()).unwrap();
        let transaction = block.transactions.last().unwrap().clone();
        block.transactions.extend(std::iter::repeat_n(transaction, count));
        corpus.push((
            name,
            RawIndexBlock {
                height: block.coinbase_height().unwrap(),
                hash: block.hash(),
                bytes: block.zcash_serialize_to_vec().unwrap(),
                txids: block.transactions.iter().map(|tx| tx.hash()).collect(),
            },
        ));
    }
    corpus
}

fn parse_block_bytes(c: &mut Criterion) {
    let blocks = corpus();
    let mut group = c.benchmark_group("parse_block");
    for (name, raw) in &blocks {
        group.throughput(Throughput::Bytes(raw.bytes.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(name), raw, |b, raw| {
            b.iter(|| parse_block(raw).unwrap())
        });
    }
    group.finish();
}

fn criterion_config() -> Criterion {
    Criterion::default().noise_threshold(0.05).sample_size(50)
}

criterion_group! {
    name = benches;
    config = criterion_config();
    targets = parse_block_bytes
}
criterion_main!(benches);
