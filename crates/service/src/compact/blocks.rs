//! Block and block-range requests, served from the volatile head and the compact index.

use std::{sync::Arc, time::Instant};

use tokio_stream::StreamExt;
use tonic::Status;
use zakura_chain::block;
use ztreamer_indexer::codec::{EncodedBlockRecord, ProtobufBlockRecord, StoredBlock};
use ztreamer_protocol::{EncodedCompactBlock, proto};

use super::{CompactService, RpcStream, ensure_ready, range_cursor::RangeCursor, reorganized};
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

    /// Answers `GetBlock`, or `GetBlockNullifiers` when `nullifiers` is set.
    pub async fn encoded_block(
        &self,
        request: proto::BlockId,
        nullifiers: bool,
    ) -> Result<EncodedCompactBlock, Status> {
        if nullifiers {
            return self.block(request, true).await.map(Into::into);
        }
        Ok(self.record_as::<EncodedBlockRecord>(request).await?.block)
    }

    /// Answers `GetBlockRange`, or `GetBlockRangeNullifiers` when `nullifiers` is set.
    pub async fn encoded_range(
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
        let backend = Arc::clone(&self.backend);
        // Transparent data is deliberately fetched on demand, outside the compact index.
        // Obviously this is extremely slow. Transparent scanning is not a recommended use case.
        Ok(Box::pin(blocks.then(move |block| {
            let backend = Arc::clone(&backend);
            async move {
                let mut block = block?;
                let hash = block::Hash(
                    block
                        .hash
                        .as_slice()
                        .try_into()
                        .map_err(|_| Status::internal("invalid compact block hash"))?,
                );
                let full =
                    backend.block(hash).await?.ok_or_else(|| reorganized(block.height as u32))?;
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
        // Bound active streams so large ranges do not all compete for throughput.
        // The permit lasts until the response stream is dropped, including transport
        // backpressure. Each stream buffers one chunk; its LMDB transaction ends
        // with its index snapshot, before yielding to the transport.
        let cursor = RangeCursor::new(Arc::clone(&self.index), snapshot, start, end, permit);
        Ok(Box::pin(tokio_stream::iter(cursor.map(move |record| record.map(&project)))))
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
mod tests;
