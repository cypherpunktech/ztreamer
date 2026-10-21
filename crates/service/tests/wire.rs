//! Compare the production wire server with the ordinary protobuf projection.

use prost::Message;
use tokio_stream::StreamExt;
use ztreamer_protocol::proto;

#[path = "../benches/support/mod.rs"]
mod support;

#[tokio::test]
#[allow(deprecated)] // Verify compatibility with the legacy nullifier RPCs too.
async fn wire_server_preserves_blocks_filters_and_boundaries() {
    let server = support::LocalServer::start().await;
    let reference = support::Fixture::new().await;
    let mut client = support::Client::connect(server.endpoint.clone()).await.unwrap();
    for (start, end) in
        [(100, 199), (199, 100), (950, 1049), (1950, 2049), (2150, 2209), (2209, 2150)]
    {
        for pools in [
            vec![],
            vec![proto::PoolType::Orchard as i32],
            vec![proto::PoolType::Sapling as i32, proto::PoolType::Ironwood as i32],
            vec![
                proto::PoolType::Orchard as i32,
                proto::PoolType::Sapling as i32,
                proto::PoolType::Ironwood as i32,
                proto::PoolType::Orchard as i32,
            ],
        ] {
            for nullifiers in [false, true] {
                let mut request = support::range(start, end);
                request.pool_types = pools.clone();
                let mut expected =
                    reference.service.range(request.clone(), nullifiers).await.unwrap();
                let mut actual = if nullifiers {
                    client.get_block_range_nullifiers(request).await
                } else {
                    client.get_block_range(request).await
                }
                .unwrap()
                .into_inner();
                while let Some(expected) = expected.next().await {
                    let expected = expected.unwrap();
                    let actual = actual.message().await.unwrap().expect("complete stream");
                    assert_eq!(actual.encode_to_vec(), expected.encode_to_vec());
                }
                assert!(actual.message().await.unwrap().is_none());
            }
        }
    }
    for height in [199, 2099, 2209] {
        for by_hash in [false, true] {
            let request = proto::BlockId {
                height: height.into(),
                hash: if by_hash { support::fixtures::hash(height).to_vec() } else { vec![] },
            };
            let expected = reference.service.block(request.clone(), false).await.unwrap();
            assert_eq!(client.get_block(request.clone()).await.unwrap().into_inner(), expected);
            let expected = reference.service.block(request.clone(), true).await.unwrap();
            assert_eq!(client.get_block_nullifiers(request).await.unwrap().into_inner(), expected);
        }
    }
    for invalid in [0, 99] {
        let mut request = support::range(0, 10);
        request.pool_types = vec![invalid];
        assert_eq!(
            client.get_block_range(request).await.unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }
    server.stop().await;
}

#[tokio::test]
async fn cancelled_streams_leave_the_server_usable() {
    let server = support::LocalServer::start().await;
    let mut tasks = tokio::task::JoinSet::new();
    for worker in 0..64 {
        let endpoint = server.endpoint.clone();
        tasks.spawn(async move {
            // Client clones share a connection with bounded HTTP/2 reset tracking.
            // Give each worker its own connection for the cancellation workload.
            let mut client = support::Client::connect(endpoint)
                .await
                .unwrap_or_else(|error| panic!("worker {worker} failed to connect: {error}"));
            for cancellation in 0..8 {
                let mut stream = client
                    .get_block_range(support::range(0, 1999))
                    .await
                    .unwrap_or_else(|error| {
                        panic!(
                            "worker {worker}, cancellation {cancellation}: request failed: {error}"
                        )
                    })
                    .into_inner();
                let first = stream.message().await.unwrap_or_else(|error| {
                    panic!("worker {worker}, cancellation {cancellation}: read failed: {error}")
                });
                assert_eq!(
                    first.map(|block| block.height),
                    Some(0),
                    "worker {worker}, cancellation {cancellation}"
                );
                // Cancel while the server still has buffered range output.
                drop(stream);
            }
            let observed =
                support::consume(&mut client, support::range(0, 15), std::time::Duration::ZERO)
                    .await
                    .unwrap_or_else(|error| {
                        panic!("worker {worker} could not reuse its channel: {error}")
                    });
            assert_eq!(observed.blocks, 16, "worker {worker}");
        });
    }
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while let Some(result) = tasks.join_next().await {
            result.expect("cancellation worker must complete");
        }
        let mut client = support::Client::connect(server.endpoint.clone())
            .await
            .expect("server must accept a fresh connection after cancellations");
        let observed =
            support::consume(&mut client, support::range(0, 1999), std::time::Duration::ZERO)
                .await
                .expect("fresh client must read a complete range after cancellations");
        assert_eq!(observed.blocks, 2000);
    })
    .await
    .expect("cancellation must release stream resources");
    server.stop().await;
}
