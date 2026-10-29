//! The Replicator gRPC service: a ztreamer serves its sealed ranges to a replica.
//!
//! [`ReplicatorService`] lists each sealed range that has a `range_meta` row, and streams the
//! packed blob of one range in chunks of [`CHUNK_BYTES`]. A client resumes a broken transfer from
//! a byte offset. A range whose row is [`VerifyState::Rejected`] is not listed and not sent.
//!
//! Each request reads the index at the generation of the current serving snapshot. If the index
//! has moved to a different generation, the request fails with `UNAVAILABLE`, and the client tries
//! again. `GetRange` reads the `range_meta` row and the blob in one LMDB snapshot, so the row that
//! it checks is the row of the blob that it sends. Each LMDB read runs on a blocking thread. At
//! most [`MAX_RANGE_STREAMS`] `GetRange` streams run at a time, because each one holds its blob in
//! memory.

use std::sync::Arc;

use prost::bytes::Bytes;
use tokio::sync::{Semaphore, watch};
use tonic::{Request, Response, Status};
use zakura_chain::parameters::Network;
use ztreamer_indexer::index::{Index, IndexError, range_meta::VerifyState};
use ztreamer_protocol::{
    RpcStream,
    replicate::{
        Empty, ExportInfo, GetRangeRequest, ListRangesRequest, ManifestEntry, RangeChunk,
        replicator_server::Replicator,
    },
};
use ztreamer_snapshot::manifest_entry::MANIFEST_VERSION;
use ztreamer_sync::head::ServingSnapshot;

/// The largest `data` of one [`RangeChunk`].
pub(crate) const CHUNK_BYTES: usize = 1 << 20;
/// The number of `GetRange` streams that can run at a time. A request above it fails with
/// `RESOURCE_EXHAUSTED`.
pub const MAX_RANGE_STREAMS: usize = 4;

/// Serves the sealed ranges of an index over the Replicator gRPC service.
///
/// `ztreamerd` serves it on the CompactTxStreamer listener:
///
/// ```ignore
/// let replicator = ReplicatorService::new(index, compact.subscribe(), &network);
/// Server::builder()
///     .add_service(CompactTxStreamerServer::new(compact))
///     .add_service(ReplicatorServer::new(replicator))
///     .serve(address)
///     .await?;
/// ```
#[derive(Clone, Debug)]
pub struct ReplicatorService {
    index: Arc<Index>,
    snapshots: watch::Receiver<ServingSnapshot>,
    network: Network,
    range_streams: Arc<Semaphore>,
}

impl ReplicatorService {
    /// A service over `index`, which was opened for `network`. Reads follow the generation of the
    /// newest snapshot in `snapshots`.
    pub fn new(
        index: Arc<Index>,
        snapshots: watch::Receiver<ServingSnapshot>,
        network: &Network,
    ) -> Self {
        Self {
            index,
            snapshots,
            network: network.clone(),
            range_streams: Arc::new(Semaphore::new(MAX_RANGE_STREAMS)),
        }
    }

    /// The generation of the current serving snapshot. Refuses while deep reorg recovery runs.
    fn generation(&self) -> Result<u64, Status> {
        self.snapshots
            .borrow()
            .ready_generation()
            .map_err(|error| Status::unavailable(error.to_string()))
    }

    /// Runs `read` against the index on a blocking thread.
    async fn read<T: Send + 'static>(
        &self,
        read: impl FnOnce(&Index) -> Result<T, IndexError> + Send + 'static,
    ) -> Result<T, Status> {
        let index = Arc::clone(&self.index);
        tokio::task::spawn_blocking(move || read(&index))
            .await
            .map_err(|error| Status::internal(format!("index read task failed: {error}")))?
            .map_err(Status::from)
    }
}

#[tonic::async_trait]
impl Replicator for ReplicatorService {
    type ListRangesStream = RpcStream<ManifestEntry>;
    type GetRangeStream = RpcStream<RangeChunk>;

    async fn get_export_info(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<ExportInfo>, Status> {
        let generation = self.generation()?;
        let state = self
            .read(move |index| {
                index.snapshot(generation).map(|index_snapshot| index_snapshot.state())
            })
            .await?;
        Ok(Response::new(ExportInfo {
            network: self.network.to_string(),
            genesis_hash: self.network.genesis_hash().0.to_vec(),
            manifest_version: MANIFEST_VERSION,
            sealed_through: state.sealed_through(),
        }))
    }

    async fn list_ranges(
        &self,
        request: Request<ListRangesRequest>,
    ) -> Result<Response<Self::ListRangesStream>, Status> {
        let generation = self.generation()?;
        let start = request.into_inner().from_start_height;
        let metas =
            self.read(move |index| index.snapshot(generation)?.range_metas_from(start)).await?;
        let entries = metas
            .into_iter()
            .filter(|meta| meta.state != VerifyState::Rejected)
            .map(|meta| Ok(meta.entry.into()))
            .collect::<Vec<_>>();
        Ok(Response::new(Box::pin(tokio_stream::iter(entries))))
    }

    async fn get_range(
        &self,
        request: Request<GetRangeRequest>,
    ) -> Result<Response<Self::GetRangeStream>, Status> {
        let GetRangeRequest { start_height: start, offset } = request.into_inner();
        let generation = self.generation()?;
        // Take the permit before the read: it bounds the blobs in memory.
        let permit = Arc::clone(&self.range_streams).try_acquire_owned().map_err(|_| {
            Status::resource_exhausted(format!("{MAX_RANGE_STREAMS} range streams are running"))
        })?;
        let blob = self
            .read(move |index| {
                let index_snapshot = index.snapshot(generation)?;
                let Some(meta) = index_snapshot.range_meta(start)? else {
                    return Ok(None);
                };
                Ok(index_snapshot.sealed_range(start)?.map(|blob| (meta, blob)))
            })
            .await?
            .filter(|(meta, _)| meta.state != VerifyState::Rejected)
            .map(|(_, blob)| Bytes::from(blob))
            .ok_or_else(|| {
                Status::not_found(format!("no listed sealed range starts at {start}"))
            })?;
        let first =
            usize::try_from(offset).ok().filter(|&first| first <= blob.len()).ok_or_else(|| {
                Status::out_of_range(format!("offset {offset} is past the blob end {}", blob.len()))
            })?;
        // The permit lasts until the response stream is dropped.
        let chunks = (first..blob.len()).step_by(CHUNK_BYTES).map(move |at| {
            let _ = &permit;
            let end = blob.len().min(at + CHUNK_BYTES);
            Ok(RangeChunk { data: blob.slice(at..end) })
        });
        Ok(Response::new(Box::pin(tokio_stream::iter(chunks))))
    }
}

#[cfg(test)]
mod tests;
