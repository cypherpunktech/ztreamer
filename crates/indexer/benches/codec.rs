// Disabled due to warnings in criterion macros
#![allow(missing_docs)]

use ztreamer_indexer::test_utils::{self as fixtures, record};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use zakura_test::vectors::BLOCK_MAINNET_1687121_BYTES;
use ztreamer_indexer::{
    codec::{CompactBlockRecord, decode_range_record, encode_range},
    index::RANGE_SIZE,
    parser::parse_block,
    types::{CompactTransaction, RawIndexBlock},
};

fn shielded_transactions() -> Vec<CompactTransaction> {
    let block = RawIndexBlock::from_bytes(BLOCK_MAINNET_1687121_BYTES.to_vec()).unwrap();
    parse_block(&block).unwrap().transactions
}

fn block_codec(c: &mut Criterion) {
    let records = [
        ("empty", record(0, Vec::new())),
        ("shielded", record(0, shielded_transactions())),
        ("mixed-heavy-v1", fixtures::mixed_records(10).pop().unwrap()),
    ];

    let mut group = c.benchmark_group("encode");
    for (name, record) in &records {
        group.throughput(Throughput::Bytes(record.encode().unwrap().len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(name), record, |b, record| {
            b.iter(|| record.encode().unwrap())
        });
    }
    group.finish();

    let mut group = c.benchmark_group("decode");
    for (name, record) in &records {
        let encoded = record.encode().unwrap();
        group.throughput(Throughput::Bytes(encoded.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(name), &encoded, |b, encoded| {
            b.iter(|| CompactBlockRecord::decode(encoded).unwrap())
        });
    }
    group.finish();
}

fn range_codec(c: &mut Criterion) {
    let transactions = shielded_transactions();
    let records =
        (0..RANGE_SIZE).map(|height| record(height, transactions.clone())).collect::<Vec<_>>();
    let encoded = encode_range(&records).unwrap();

    let mut group = c.benchmark_group("encode_range");
    group.throughput(Throughput::Bytes(encoded.len() as u64));
    group.bench_function(format!("{RANGE_SIZE}-blocks"), |b| {
        b.iter(|| encode_range(&records).unwrap())
    });
    group.finish();

    c.bench_function("decode_range_record", |b| {
        b.iter(|| decode_range_record(&encoded, RANGE_SIZE as usize / 2).unwrap())
    });
}

fn mixed_range_codec(c: &mut Criterion) {
    let records = fixtures::mixed_records(RANGE_SIZE);
    let encoded = encode_range(&records).unwrap();
    for index in [0, 500, 999] {
        assert_eq!(decode_range_record(&encoded, index).unwrap(), records[index]);
    }
    let mut group = c.benchmark_group("range_codec_mixed_v1");
    group.throughput(Throughput::Bytes(encoded.len() as u64));
    group.bench_function("encode", |b| b.iter(|| encode_range(&records).unwrap()));
    for index in [0, 500, 999] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("decode", index), &index, |b, &index| {
            b.iter(|| decode_range_record(&encoded, index).unwrap())
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
    targets = block_codec, range_codec, mixed_range_codec
}
criterion_main!(benches);
