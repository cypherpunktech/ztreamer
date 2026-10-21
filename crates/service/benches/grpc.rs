#![allow(missing_docs)]

mod support;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::{sync::Arc, time::Duration};
use support::{Client, LocalServer, consume, range};
use tokio::{sync::Barrier, task::JoinSet};

fn grpc(c: &mut Criterion) {
    let runtime =
        tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
    let server = runtime.block_on(LocalServer::start());
    let clients = runtime.block_on(async {
        let mut clients = Vec::new();
        // Each simulated wallet owns a connection; cloning a Tonic client shares it.
        for _ in 0..32 {
            clients.push(Client::connect(server.endpoint.clone()).await.unwrap());
        }
        clients
    });
    for (name, delay) in
        [("grpc_mixed_v1", Duration::ZERO), ("grpc_slow_mixed_v1", Duration::from_millis(1))]
    {
        let mut group = c.benchmark_group(name);
        for concurrency in [1, 8, 16, 32] {
            group.throughput(Throughput::Elements(2_000 * concurrency as u64));
            group.bench_with_input(
                BenchmarkId::new("2000-blocks", concurrency),
                &concurrency,
                |b, &concurrency| {
                    b.to_async(&runtime).iter(|| async {
                        let barrier = Arc::new(Barrier::new(concurrency));
                        let mut tasks = JoinSet::new();
                        for mut client in clients[..concurrency].iter().cloned() {
                            let barrier = barrier.clone();
                            tasks.spawn(async move {
                                barrier.wait().await;
                                tokio::time::timeout(
                                    Duration::from_secs(30),
                                    consume(&mut client, range(0, 1_999), delay),
                                )
                                .await
                                .unwrap()
                                .unwrap()
                            });
                        }
                        while let Some(result) = tasks.join_next().await {
                            std::hint::black_box(result.unwrap());
                        }
                    });
                },
            );
        }
        group.finish();
    }
    drop(clients);
    runtime.block_on(server.stop());
}

criterion_group! { name = benches; config = Criterion::default().noise_threshold(0.05).sample_size(20); targets = grpc }
criterion_main!(benches);
