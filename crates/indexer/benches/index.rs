#![allow(missing_docs)]

#[path = "../../../benchmarks/fixtures.rs"]
mod fixtures;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ztreamer_indexer::{
    index::{BlockId, Index, IndexState},
    ingest::OrderedBuilder,
};

fn open(dir: &tempfile::TempDir) -> Index {
    Index::open(dir.path(), 512 * 1024 * 1024, "Mainnet", [9; 32]).unwrap()
}

fn builder(c: &mut Criterion) {
    let records = fixtures::mixed_records(1_000);
    let mut group = c.benchmark_group("builder_mixed_v1");
    group.throughput(Throughput::Elements(1_000));
    for reversed in [false, true] {
        group.bench_function(if reversed { "out-of-order" } else { "ordered" }, |b| {
            b.iter_batched_ref(
                || {
                    let mut parsed: Vec<_> =
                        records.iter().cloned().map(fixtures::parsed).collect();
                    if reversed {
                        parsed.reverse();
                    }
                    (OrderedBuilder::new(IndexState::default(), 256 * 1024 * 1024).unwrap(), parsed)
                },
                |(builder, parsed)| {
                    for block in parsed.drain(..) {
                        builder.push(block).unwrap();
                    }
                    std::hint::black_box(
                        builder
                            .build_batch(Some(999), Some(999), 16 * 1024 * 1024)
                            .unwrap()
                            .unwrap(),
                    );
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

fn storage(c: &mut Criterion) {
    let records = fixtures::mixed_records(2_200);
    let mut group = c.benchmark_group("write_mixed_v1");
    for (name, count, seal) in [("mutable", 100, None), ("sealed", 1_000, Some(999))] {
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::new(name, count), &count, |b, &count| {
            b.iter_batched_ref(
                || {
                    let dir = tempfile::tempdir().unwrap();
                    let index = open(&dir);
                    let mut builder =
                        OrderedBuilder::new(IndexState::default(), 256 * 1024 * 1024).unwrap();
                    for record in &records[..count] {
                        builder.push(fixtures::parsed(record.clone())).unwrap();
                    }
                    let batch = builder
                        .build_batch(Some(count as u32 - 1), seal, 16 * 1024 * 1024)
                        .unwrap()
                        .unwrap();
                    (index, dir, Some(batch))
                },
                |(index, _dir, batch)| {
                    let state = index.write(batch.take().unwrap()).unwrap();
                    assert_eq!(state.durable_tip().unwrap().height, count as u32 - 1);
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();

    let mut group = c.benchmark_group("reorg_mixed_v1");
    for (name, ancestor) in [("mutable-100", 2_099_u32), ("deep-1200", 999)] {
        group.throughput(Throughput::Elements(u64::from(2_199 - ancestor)));
        group.bench_function(name, |b| {
            b.iter_batched_ref(
                || {
                    let dir = tempfile::tempdir().unwrap();
                    let index = open(&dir);
                    let state = fixtures::populate(&index, &records, Some(1_999));
                    let mut replacement = records[ancestor as usize + 1..].to_vec();
                    let mut previous = fixtures::hash(ancestor);
                    for record in &mut replacement {
                        record.previous_hash = previous;
                        record.hash[31] = 1;
                        previous = record.hash;
                    }
                    (index, dir, state, Some(replacement))
                },
                |(index, _dir, state, replacement)| {
                    let anchor = BlockId::new(ancestor, fixtures::hash(ancestor));
                    let replacement = replacement.take().unwrap();
                    let result = if ancestor >= 1_999 {
                        index.replace_mutable_suffix(
                            state.generation(),
                            anchor,
                            replacement,
                            Some(1_999),
                        )
                    } else {
                        index.replace_deep_suffix(
                            state.generation(),
                            anchor,
                            replacement,
                            Some(1_999),
                        )
                    }
                    .unwrap();
                    assert_eq!(result.durable_tip().unwrap().hash[31], 1);
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();

    let dir = tempfile::tempdir().unwrap();
    let index = open(&dir);
    fixtures::populate(&index, &records, Some(1_999));
    drop(index);
    c.bench_function("restart_mixed_v1/2200-blocks-warm", |b| b.iter(|| open(&dir)));
}

criterion_group! { name = benches; config = Criterion::default().noise_threshold(0.05).sample_size(20); targets = builder, storage }
criterion_main!(benches);
