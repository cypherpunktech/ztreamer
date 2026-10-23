//! Block and block-range requests, served from the volatile head and the compact index.

use std::{sync::Arc, time::Instant};

use tokio_stream::StreamExt;
use tonic::Status;
use tower::ServiceExt;
use zakura_chain::block;
use zakura_state::{ReadRequest, ReadResponse};
use ztreamer_indexer::{
    Digest,
    codec::{EncodedBlockRecord, ProtobufBlockRecord, StoredBlock},
    index::IndexError,
};
use ztreamer_protocol::{EncodedCompactBlock, proto};

use super::{CompactService, RpcStream, ensure_ready, index_status, reorganized, source_status};
use crate::project::{PoolSelection, add_transparent_txs, project_block};

impl CompactService {
    /// Returns a decoded block, optionally projected to nullifiers.
    pub async fn block(
        &self,
        request: proto::BlockId,
        nullifiers: bool,
    ) -> Result<proto::CompactBlock, Status> {
        let record = self.record_as::<ProtobufBlockRecord>(request).await?;
        let pools = PoolSelection::from_request(&[]).expect("empty pool selection is valid");
        Ok(project_block(record.0, pools, nullifiers))
    }

    pub(crate) async fn encoded_block(
        &self,
        request: proto::BlockId,
        nullifiers: bool,
    ) -> Result<EncodedCompactBlock, Status> {
        if nullifiers {
            return self.block(request, true).await.map(Into::into);
        }
        Ok(self.record_as::<EncodedBlockRecord>(request).await?.block)
    }

    pub(crate) async fn encoded_range(
        &self,
        request: proto::BlockRange,
        nullifiers: bool,
    ) -> Result<RpcStream<EncodedCompactBlock>, Status> {
        let guard = self.metrics.range();
        guard.response(self.encoded_range_inner(request, nullifiers).await)
    }

    async fn encoded_range_inner(
        &self,
        request: proto::BlockRange,
        nullifiers: bool,
    ) -> Result<RpcStream<EncodedCompactBlock>, Status> {
        let pools = PoolSelection::from_request(&request.pool_types)?;
        if !nullifiers && pools.is_all() {
            self.range_as(request, |record: EncodedBlockRecord| record.block).await
        } else {
            Ok(Box::pin(
                self.range_inner(request, nullifiers).await?.map(|block| block.map(Into::into)),
            ))
        }
    }

    /// Streams decoded blocks with the requested pool/nullifier projection.
    pub async fn range(
        &self,
        request: proto::BlockRange,
        nullifiers: bool,
    ) -> Result<RpcStream<proto::CompactBlock>, Status> {
        let guard = self.metrics.range();
        guard.response(self.range_inner(request, nullifiers).await)
    }

    async fn range_inner(
        &self,
        request: proto::BlockRange,
        nullifiers: bool,
    ) -> Result<RpcStream<proto::CompactBlock>, Status> {
        let pools = PoolSelection::from_request(&request.pool_types)?;
        let blocks = self
            .range_as(request, move |record: ProtobufBlockRecord| {
                project_block(record.0, pools, nullifiers)
            })
            .await?;
        if !pools.transparent {
            return Ok(blocks);
        }
        let source = self.zakura.clone();
        // Transparent data is deliberately fetched on demand, outside the compact index.
        // Obviously this is extremely slow. Transparent scanning is not a recommended use case.
        Ok(Box::pin(blocks.then(move |block| {
            let source = source.clone();
            async move {
                let mut block = block?;
                let hash = block::Hash(
                    block
                        .hash
                        .as_slice()
                        .try_into()
                        .map_err(|_| Status::internal("invalid compact block hash"))?,
                );
                let response =
                    source.oneshot(ReadRequest::Block(hash.into())).await.map_err(source_status)?;
                let full = match response {
                    ReadResponse::Block(Some(block)) => block,
                    ReadResponse::Block(None) => return Err(reorganized(block.height as u32)),
                    _ => return Err(Status::internal("unexpected block response")),
                };
                add_transparent_txs(&mut block, &full.transactions);
                Ok(block)
            }
        })))
    }

    async fn range_as<T: StoredBlock + Send + 'static, B: Send + 'static>(
        &self,
        request: proto::BlockRange,
        project: impl Fn(T) -> B + Send + 'static,
    ) -> Result<RpcStream<B>, Status> {
        let (start, end) = range_heights(&request)?;
        let snapshot = self.snapshot();
        ensure_ready(&snapshot)?;
        let visible_tip =
            snapshot.visible_tip.ok_or_else(|| Status::unavailable("compact index is empty"))?;
        if start > visible_tip.height || end > visible_tip.height {
            return Err(Status::out_of_range(format!(
                "range exceeds compact tip {}",
                visible_tip.height
            )));
        }
        if !snapshot.tip_fresh
            && snapshot
                .durable_tip
                .is_none_or(|durable| start > durable.height || end > durable.height)
        {
            return Err(Status::unavailable("canonical head source is stale"));
        }
        let admission_started = Instant::now();
        let permit = Arc::clone(&self.range_streams).acquire_owned().await;
        self.metrics.admission.record(admission_started.elapsed().as_secs_f64());
        let permit = permit.map_err(|_| Status::unavailable("range stream pool is closed"))?;
        let index = Arc::clone(&self.index);
        let ascending = start <= end;
        let durable_tip = snapshot.durable_tip.map(|tip| tip.height);
        let mut cursor = Some(start);
        // Bound active streams so large ranges do not all compete for throughput.
        // The permit lasts until the response stream is dropped, including transport
        // backpressure. Each stream buffers one chunk; its LMDB transaction ends
        // inside read_range_latest_as before yielding to the transport.
        let mut records = Vec::<T>::new().into_iter();
        // (hash, previous_hash) of the last record sent; every chunk must chain onto it.
        let mut last: Option<(Digest, Digest)> = None;
        Ok(Box::pin(tokio_stream::iter(std::iter::from_fn(move || {
            loop {
                let _ = &permit;
                if let Some(record) = records.next() {
                    last = Some((record.hash(), record.previous_hash()));
                    return Some(Ok(project(record)));
                }
                let height = cursor?;
                let mut chunk_end = if ascending {
                    height.saturating_add(63).min(end)
                } else {
                    height.saturating_sub(63).max(end)
                };
                let durable = durable_tip.is_some_and(|tip| height <= tip);
                if let Some(tip) = durable_tip {
                    if ascending && durable {
                        chunk_end = chunk_end.min(tip);
                    } else if !ascending && !durable {
                        chunk_end = chunk_end.max(tip.saturating_add(1));
                    }
                }
                let mut chunk = Vec::with_capacity(64);
                let result = if durable {
                    index.read_range_latest_as(height, chunk_end, |record| {
                        chunk.push(record);
                        true
                    })
                } else {
                    (0..=height.abs_diff(chunk_end)).try_for_each(|offset| {
                        let height = if ascending { height + offset } else { height - offset };
                        chunk.push(T::from_record(
                            snapshot.volatile(height).ok_or(IndexError::Coverage { height })?,
                        ));
                        Ok(())
                    })
                };
                if let Err(error) = result {
                    cursor = None;
                    return Some(Err(match error {
                        // Validation saw this height; if the index no longer has it, the chain shrank.
                        IndexError::Coverage { height } if durable => reorganized(height),
                        other => index_status(other),
                    }));
                }
                if let Some((last_hash, last_previous)) = last
                    && let Some(first) = chunk.first()
                {
                    let continuous = if ascending {
                        first.previous_hash() == last_hash
                    } else {
                        last_previous == first.hash()
                    };
                    if !continuous {
                        cursor = None;
                        return Some(Err(reorganized(first.height())));
                    }
                }
                cursor = (chunk_end != end).then_some(if ascending {
                    chunk_end + 1
                } else {
                    chunk_end - 1
                });
                records = chunk.into_iter();
            }
        }))))
    }
}

fn range_heights(request: &proto::BlockRange) -> Result<(u32, u32), Status> {
    let endpoint = |name, endpoint: &Option<proto::BlockId>| {
        let endpoint = endpoint
            .as_ref()
            .ok_or_else(|| Status::invalid_argument(format!("range.{name} is required")))?;
        if !endpoint.hash.is_empty() {
            return Err(Status::invalid_argument("range endpoints must be heights"));
        }
        u32::try_from(endpoint.height)
            .map_err(|_| Status::invalid_argument("range height exceeds u32"))
    };
    Ok((endpoint("start", &request.start)?, endpoint("end", &request.end)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tonic::Request;
    use zakura_chain::parameters::Network;
    use zakura_state::Config;
    use ztreamer_indexer::{
        codec::CompactBlockRecord,
        index::{BlockId, Index},
        ingest::OrderedBuilder,
    };
    use ztreamer_protocol::wire::compact_tx_streamer_server::CompactTxStreamer;

    use crate::service::{
        MAX_RANGE_STREAMS, Readiness,
        test_utils::{hash, index_through, parsed, range, record},
    };

    #[test]
    fn streams_cross_range_descending_and_fetches_transparent_data() {
        tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap().block_on(
            async {
                let dir = tempfile::tempdir().unwrap();
                let index = Arc::new(
                    Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", [9; 32]).unwrap(),
                );
                let state = index_through(&index, 1_005);
                let (_state_service, read_service, _tip, _change) = zakura_state::init(
                    Config::ephemeral(),
                    &Network::Mainnet,
                    block::Height::MAX,
                    0,
                )
                .await
                .expect("ephemeral state initializes");
                let service = CompactService::new(index, state, "main", read_service);
                assert_eq!(
                    service.readiness(),
                    Readiness {
                        historical: true,
                        tip: false,
                        recovering: false,
                        source_error: None,
                    }
                );
                let descending = range(1_002, 998);
                let mut stream = service
                    .get_block_range(Request::new(descending.clone()))
                    .await
                    .unwrap()
                    .into_inner();
                let mut heights = Vec::new();
                while let Some(block) = stream.next().await {
                    heights.push(block.unwrap().into_decoded().unwrap().height);
                }
                assert_eq!(heights, [1_002, 1_001, 1_000, 999, 998]);

                service.publish_head(state, vec![record(1_006), record(1_007)]).unwrap();
                assert!(service.readiness().tip);
                let mut stream = service
                    .get_block_range(Request::new(range(1_004, 1_007)))
                    .await
                    .unwrap()
                    .into_inner();
                let mut heights = Vec::new();
                while let Some(block) = stream.next().await {
                    heights.push(block.unwrap().into_decoded().unwrap().height);
                }
                assert_eq!(heights, [1_004, 1_005, 1_006, 1_007]);

                let recovery_range = range(1_004, 1_007);
                let mut pinned = service
                    .get_block_range(Request::new(recovery_range.clone()))
                    .await
                    .unwrap()
                    .into_inner();
                service.begin_recovery();
                assert_eq!(
                    service
                        .get_block_range(Request::new(recovery_range))
                        .await
                        .err()
                        .unwrap()
                        .code(),
                    tonic::Code::Unavailable
                );
                let mut heights = Vec::new();
                while let Some(block) = pinned.next().await {
                    heights.push(block.unwrap().into_decoded().unwrap().height);
                }
                assert_eq!(heights, [1_004, 1_005, 1_006, 1_007]);
                service.publish_head(state, vec![record(1_006), record(1_007)]).unwrap();
                service.mark_source_failure("offline".to_owned(), Duration::ZERO);
                assert!(!service.readiness().tip);
                assert_eq!(
                    service
                        .get_latest_block(Request::new(proto::ChainSpec::default()))
                        .await
                        .unwrap_err()
                        .code(),
                    tonic::Code::Unavailable
                );
                assert!(
                    service
                        .get_block(Request::new(proto::BlockId { height: 1_000, hash: Vec::new() }))
                        .await
                        .is_ok()
                );
                assert_eq!(
                    service
                        .get_transaction(Request::new(proto::TxFilter {
                            hash: vec![0; 32],
                            ..Default::default()
                        }))
                        .await
                        .unwrap_err()
                        .code(),
                    tonic::Code::Unavailable
                );

                let mut transparent = descending;
                transparent.pool_types = vec![proto::PoolType::Transparent as i32];
                assert_eq!(
                    service
                        .get_block_range(Request::new(transparent))
                        .await
                        .unwrap()
                        .into_inner()
                        .next()
                        .await
                        .unwrap()
                        .unwrap_err()
                        .code(),
                    tonic::Code::Unavailable
                );
                assert_eq!(
                    CompactTxStreamer::send_transaction(
                        &service,
                        Request::new(proto::RawTransaction::default()),
                    )
                    .await
                    .unwrap_err()
                    .code(),
                    tonic::Code::InvalidArgument
                );
                assert_eq!(
                    CompactTxStreamer::get_mempool_tx(
                        &service,
                        Request::new(proto::GetMempoolTxRequest {
                            pool_types: vec![proto::PoolType::Transparent as i32],
                            ..Default::default()
                        }),
                    )
                    .await
                    .err()
                    .unwrap()
                    .code(),
                    tonic::Code::Unavailable
                );
                assert_eq!(
                    CompactTxStreamer::get_mempool_tx(
                        &service,
                        Request::new(proto::GetMempoolTxRequest::default()),
                    )
                    .await
                    .err()
                    .unwrap()
                    .code(),
                    tonic::Code::Unavailable
                );
                assert_eq!(
                    CompactTxStreamer::get_mempool_stream(&service, Request::new(proto::Empty {}),)
                        .await
                        .err()
                        .unwrap()
                        .code(),
                    tonic::Code::Unavailable
                );

                service.snapshot.send_modify(|snapshot| {
                    snapshot.visible_tip = Some(BlockId::new(3_464_754, [0; 32]));
                });
                let info = service.lightd_info();
                assert_eq!(info.sapling_activation_height, 419_200);
                assert_eq!(info.consensus_branch_id, "37a5165b");
                assert_eq!(info.block_height, 3_464_754);
                assert!(!info.zcashd_build.is_empty());
                assert!(!info.zcashd_subversion.is_empty());
            },
        );
    }

    #[test]
    fn range_admission_is_shared_and_releases_on_drop() {
        tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap().block_on(
            async {
                let dir = tempfile::tempdir().unwrap();
                let index = Arc::new(
                    Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", [9; 32]).unwrap(),
                );
                let state = index_through(&index, 200);
                let (_state_service, read_service, _tip, _change) = zakura_state::init(
                    Config::ephemeral(),
                    &Network::Mainnet,
                    block::Height::MAX,
                    0,
                )
                .await
                .expect("ephemeral state initializes");
                let service = CompactService::new(index, state, "main", read_service);
                let clone = service.clone();
                let mut streams = Vec::new();
                for _ in 0..MAX_RANGE_STREAMS {
                    streams.push(service.encoded_range(range(0, 200), false).await.unwrap());
                }
                // Poll once to prove the limit applies across service clones.
                let waiting = clone.encoded_range(range(0, 200), false);
                tokio::pin!(waiting);
                assert!(
                    std::future::poll_fn(|cx| {
                        std::task::Poll::Ready(
                            std::future::Future::poll(waiting.as_mut(), cx).is_pending(),
                        )
                    })
                    .await
                );
                drop(streams.pop());
                let mut admitted = tokio::time::timeout(Duration::from_secs(1), waiting)
                    .await
                    .expect("dropping a response frees a slot")
                    .unwrap();
                let mut count = 0;
                while let Some(block) = admitted.next().await {
                    block.unwrap();
                    count += 1;
                }
                assert_eq!(count, 201);
                drop(admitted);
                let _next_stream = tokio::time::timeout(
                    Duration::from_secs(1),
                    service.encoded_range(range(0, 0), false),
                )
                .await
                .expect("completed response frees a slot")
                .unwrap();
            },
        );
    }

    #[test]
    fn range_stream_survives_a_durable_advance_mid_flight() {
        tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap().block_on(
            async {
                let dir = tempfile::tempdir().unwrap();
                let index = Arc::new(
                    Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", [9; 32]).unwrap(),
                );
                let state = index_through(&index, 200);
                let (_state_service, read_service, _tip, _change) = zakura_state::init(
                    Config::ephemeral(),
                    &Network::Mainnet,
                    block::Height::MAX,
                    0,
                )
                .await
                .expect("ephemeral state initializes");
                let service = CompactService::new(Arc::clone(&index), state, "main", read_service);

                let mut stream = service
                    .encoded_range(range(0, 200), false)
                    .await
                    .unwrap()
                    .map(|block| block.map(|block| block.into_decoded().unwrap()));
                assert_eq!(stream.next().await.unwrap().unwrap().height, 0);

                // A durable advance lands under the stream.
                let mut builder = OrderedBuilder::new(state, 1024 * 1024).unwrap();
                for height in 201..=205 {
                    builder.push(parsed(height)).unwrap();
                }
                let batch = builder
                    .build_batch(Some(205), Some(205), 1024 * 1024)
                    .unwrap()
                    .expect("the builder holds five blocks");
                let advanced = index.write(batch).unwrap();
                assert_eq!(advanced.generation(), state.generation() + 1);

                let mut heights = vec![0];
                while let Some(block) = stream.next().await {
                    heights.push(block.unwrap().height);
                }
                assert_eq!(heights, (0..=200).collect::<Vec<_>>());
            },
        );
    }

    #[test]
    fn range_stream_refuses_a_reorg_it_cannot_chain_onto() {
        tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap().block_on(
            async {
                let dir = tempfile::tempdir().unwrap();
                let index = Arc::new(
                    Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", [9; 32]).unwrap(),
                );
                let state = index_through(&index, 200);
                let (_state_service, read_service, _tip, _change) = zakura_state::init(
                    Config::ephemeral(),
                    &Network::Mainnet,
                    block::Height::MAX,
                    0,
                )
                .await
                .expect("ephemeral state initializes");
                let service = CompactService::new(Arc::clone(&index), state, "main", read_service);

                let mut stream = service
                    .encoded_range(range(0, 200), false)
                    .await
                    .unwrap()
                    .map(|block| block.map(|block| block.into_decoded().unwrap()));
                assert_eq!(stream.next().await.unwrap().unwrap().height, 0);

                // A competing chain replaces everything above 30; the next chunk cannot link onto 63.
                let fork_hash = |height: u32| {
                    let mut digest = hash(height);
                    digest[31] = 0xff;
                    digest
                };
                let fork: Vec<CompactBlockRecord> = (31..=205)
                    .map(|height| {
                        let mut forked = record(height);
                        forked.hash = fork_hash(height);
                        if height > 31 {
                            forked.previous_hash = fork_hash(height - 1);
                        }
                        forked
                    })
                    .collect();
                index
                    .replace_mutable_suffix(
                        state.generation(),
                        BlockId::new(30, hash(30)),
                        fork,
                        None,
                    )
                    .unwrap();

                let mut heights = vec![0];
                let mut refusal = None;
                while let Some(item) = stream.next().await {
                    match item {
                        Ok(block) => heights.push(block.height),
                        Err(status) => {
                            refusal = Some(status);
                            break;
                        }
                    }
                }
                assert_eq!(heights, (0..=63).collect::<Vec<_>>());
                assert_eq!(
                    refusal.expect("the boundary is refused").code(),
                    tonic::Code::Unavailable
                );
            },
        );
    }

    #[test]
    fn descending_range_from_the_volatile_head_survives_an_advance() {
        tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap().block_on(
            async {
                let dir = tempfile::tempdir().unwrap();
                let index = Arc::new(
                    Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", [9; 32]).unwrap(),
                );
                let state = index_through(&index, 200);
                let (_state_service, read_service, _tip, _change) = zakura_state::init(
                    Config::ephemeral(),
                    &Network::Mainnet,
                    block::Height::MAX,
                    0,
                )
                .await
                .expect("ephemeral state initializes");
                let service = CompactService::new(Arc::clone(&index), state, "main", read_service);
                service.publish_head(state, vec![record(201), record(202)]).unwrap();

                // The first chunk is volatile, the rest is durable and read after an advance.
                let mut stream = service
                    .encoded_range(range(202, 100), false)
                    .await
                    .unwrap()
                    .map(|block| block.map(|block| block.into_decoded().unwrap()));
                assert_eq!(stream.next().await.unwrap().unwrap().height, 202);
                let mut builder = OrderedBuilder::new(state, 1024 * 1024).unwrap();
                for height in 201..=205 {
                    builder.push(parsed(height)).unwrap();
                }
                let batch = builder
                    .build_batch(Some(205), Some(205), 1024 * 1024)
                    .unwrap()
                    .expect("the builder holds five blocks");
                index.write(batch).unwrap();

                let mut heights = vec![202];
                while let Some(block) = stream.next().await {
                    heights.push(block.unwrap().height);
                }
                assert_eq!(heights, (100..=202).rev().collect::<Vec<_>>());
            },
        );
    }
}
