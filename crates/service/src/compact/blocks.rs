//! Block and block-range requests, served from the volatile head and the compact index.

use std::{sync::Arc, time::Instant};

use tokio_stream::StreamExt;
use tonic::Status;
use zakura_chain::block;
use ztreamer_indexer::{
    codec::{EncodedBlockRecord, ProtobufBlockRecord, StoredBlock},
    index::IndexError,
};
use ztreamer_protocol::{EncodedCompactBlock, proto};

use super::{CompactService, RpcStream, ensure_ready, index_status, reorganized};
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
        let mut last: Option<(block::Hash, block::Hash)> = None;
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
mod tests;
