// Disabled due to warnings in criterion macros
#![allow(missing_docs)]

use std::{hint::black_box, sync::Arc};

use ztreamer_service::test_utils as support;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio_stream::StreamExt as _;
use zakura_chain::{block, parameters::Network, serialization::ZcashDeserialize as _};
use zakura_state::Config;
use zakura_test::vectors::BLOCK_MAINNET_1687121_BYTES;
use ztreamer_indexer::{
    index::{Index, IndexState},
    ingest::OrderedBuilder,
    parser::parse_block,
    test_utils::hash,
    types::{CompactTransaction, ParsedCompactBlock, RawIndexBlock},
};
use ztreamer_protocol::proto;
use ztreamer_service::CompactService;

const TIP: u32 = 2_005;

fn shielded_transactions() -> Vec<CompactTransaction> {
    let bytes = BLOCK_MAINNET_1687121_BYTES.to_vec();
    let block = block::Block::zcash_deserialize(bytes.as_slice()).unwrap();
    parse_block(&RawIndexBlock {
        height: block.coinbase_height().unwrap(),
        hash: block.hash(),
        bytes,
        txids: block.transactions.iter().map(|transaction| transaction.hash()).collect(),
    })
    .unwrap()
    .transactions
}

fn index_through(index: &Index, tip: u32) -> IndexState {
    let transactions = shielded_transactions();
    let mut builder = OrderedBuilder::new(IndexState::default(), 64 * 1024 * 1024).unwrap();
    for height in 0..=tip {
        builder
            .push(ParsedCompactBlock {
                height,
                hash: hash(height),
                previous_hash: height.checked_sub(1).map(hash).unwrap_or([0; 32]),
                time: height,
                transactions: transactions.clone(),
                sapling_additions: 0,
                orchard_additions: 0,
                ironwood_additions: 0,
            })
            .unwrap();
    }
    let mut state = IndexState::default();
    while let Some(batch) = builder.build_batch(Some(tip), Some(tip), 64 * 1024 * 1024).unwrap() {
        state = index.write(batch).unwrap();
    }
    state
}

fn block_id(height: u32) -> Option<proto::BlockId> {
    Some(proto::BlockId { height: u64::from(height), hash: Vec::new() })
}

fn get_block_range(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let index = Arc::new(Index::open(dir.path(), 512 * 1024 * 1024, "Mainnet", [9; 32]).unwrap());
    let state = index_through(&index, TIP);
    let (_state_service, read_service, _tip, _change) = runtime.block_on(async {
        zakura_state::init(Config::ephemeral(), &Network::Mainnet, block::Height::MAX, 0)
            .await
            .expect("ephemeral state initializes")
    });
    let service = CompactService::new(index, state, "main", read_service);

    let mut group = c.benchmark_group("get_block_range");
    for blocks in [100, 200, 600, 1_000, 2_000] {
        group.throughput(Throughput::Elements(u64::from(blocks)));
        for (direction, start, end) in
            [("ascending", TIP - blocks, TIP - 1), ("descending", TIP - 1, TIP - blocks)]
        {
            let range = proto::BlockRange {
                start: block_id(start),
                end: block_id(end),
                pool_types: Vec::new(),
            };
            runtime.block_on(async {
                let mut stream = service.range(range.clone(), false).await.unwrap();
                let mut count = 0;
                while let Some(block) = stream.next().await {
                    let block = block.unwrap();
                    let expected = if start <= end { start + count } else { start - count };
                    assert_eq!(block.height, u64::from(expected));
                    count += 1;
                }
                assert_eq!(count, blocks);
            });
            group.bench_with_input(BenchmarkId::new(direction, blocks), &range, |b, range| {
                b.to_async(&runtime).iter(|| async {
                    let mut stream = service.range(range.clone(), false).await.unwrap();
                    while let Some(block) = stream.next().await {
                        black_box(block.unwrap());
                    }
                })
            });
        }
    }
    group.finish();
}

fn mixed_ranges(c: &mut Criterion) {
    let runtime =
        tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
    let fixture = runtime.block_on(support::Fixture::new());
    let mut group = c.benchmark_group("range_paths_mixed_v1");
    for (name, start, end, nullifiers, pools) in [
        ("sealed", 100, 199, false, vec![]),
        ("mutable", 2_000, 2_099, false, vec![]),
        ("volatile", 2_200, 2_209, false, vec![]),
        ("sealed-boundary", 950, 1_049, false, vec![]),
        ("sealed-mutable", 1_950, 2_049, false, vec![]),
        ("durable-volatile", 2_150, 2_209, false, vec![]),
        ("volatile-durable", 2_209, 2_150, false, vec![]),
        ("nullifiers", 100, 199, true, vec![]),
        ("orchard-only", 100, 199, false, vec![proto::PoolType::Orchard as i32]),
    ] {
        let mut request = support::range(start, end);
        request.pool_types = pools;
        let service = &fixture.service;
        let run = || async {
            let mut stream = service.range(request.clone(), nullifiers).await.unwrap();
            let mut count = 0;
            while let Some(block) = stream.next().await {
                let block = block.unwrap();
                let expected = if start <= end { start + count } else { start - count };
                assert_eq!(block.height, u64::from(expected));
                black_box(block);
                count += 1;
            }
            assert_eq!(count, start.abs_diff(end) + 1);
        };
        runtime.block_on(run());
        group.throughput(Throughput::Elements(u64::from(start.abs_diff(end) + 1)));
        group.bench_function(name, |b| b.to_async(&runtime).iter(run));
    }
    group.finish();
    let mut group = c.benchmark_group("get_block_mixed_v1");
    group.throughput(Throughput::Elements(1));
    for (name, height) in [("sealed", 199), ("mutable", 2_099), ("volatile", 2_209)] {
        group.bench_function(name, |b| {
            b.to_async(&runtime).iter(|| async {
                let block = fixture.service.block(block_id(height).unwrap(), false).await.unwrap();
                assert_eq!(block.height, u64::from(height));
                black_box(block)
            })
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
    targets = get_block_range, mixed_ranges
}
criterion_main!(benches);
