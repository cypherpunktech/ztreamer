//! The Replicator gRPC service: a ztreamer serves its sealed ranges to a replica.
//!
//! [`ReplicatorService`] lists each sealed range that has a `range_meta` row, and streams the
//! packed blob of one range in chunks of [`CHUNK_BYTES`]. A client resumes a broken transfer from
//! a byte offset. A range whose row is [`VerifyState::Rejected`] is not listed and not sent.
//!
//! Each request reads the index at the generation of the current serving snapshot. If the index
//! has moved to a different generation, the request fails with `UNAVAILABLE`, and the client tries
//! again. Each LMDB read runs on a blocking thread. At most [`MAX_RANGE_STREAMS`] `GetRange`
//! streams run at a time, because each one holds its blob in memory.

use std::sync::Arc;

use prost::bytes::Bytes;
use tokio::sync::{Semaphore, watch};
use tonic::{Request, Response, Status};
use zakura_chain::parameters::Network;
use ztreamer_indexer::{
    codec::RANGE_FORMAT_VERSION,
    index::{
        Index, IndexError,
        range_meta::{ExportedRange, VerifyState},
    },
};
use ztreamer_protocol::replicate::{
    Empty, ExportInfo, GetRangeRequest, ListRangesRequest, ManifestEntry, RangeChunk,
    replicator_server::Replicator,
};

use crate::{ServingSnapshot, service::RpcStream};

/// The largest `data` of one [`RangeChunk`].
pub const CHUNK_BYTES: usize = 1 << 20;
/// The number of `GetRange` streams that can run at a time. A request above it fails with
/// `RESOURCE_EXHAUSTED`.
pub const MAX_RANGE_STREAMS: usize = 4;
/// The version of the [`ManifestEntry`] message that the service sends.
const MANIFEST_VERSION: u32 = 1;

/// Serves the sealed ranges of an index over the Replicator gRPC service.
///
/// ```ignore
/// let replicator = ReplicatorService::new(index, compact.subscribe(), &network);
/// Server::builder().add_service(ReplicatorServer::new(replicator)).serve(address).await?;
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
        let snapshot = self.snapshots.borrow();
        if !snapshot.ready {
            return Err(Status::unavailable("deep reorg recovery is active"));
        }
        Ok(snapshot.generation)
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
            .map_err(|error| match error {
                IndexError::Generation { .. } => Status::unavailable(error.to_string()),
                other => Status::internal(other.to_string()),
            })
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
                let state = index.state()?;
                if state.generation() != generation {
                    return Err(IndexError::Generation {
                        expected: generation,
                        actual: state.generation(),
                    });
                }
                Ok(state)
            })
            .await?;
        Ok(Response::new(ExportInfo {
            network: self.network.to_string(),
            genesis_hash: self.network.genesis_hash().0.to_vec(),
            range_versions: vec![RANGE_FORMAT_VERSION.into()],
            manifest_version: MANIFEST_VERSION,
            sealed_through: state.sealed_through(),
            verified_through: state.verified_through(),
            generation,
        }))
    }

    async fn list_ranges(
        &self,
        request: Request<ListRangesRequest>,
    ) -> Result<Response<Self::ListRangesStream>, Status> {
        let generation = self.generation()?;
        let start = request.into_inner().from_start_height;
        let metas = self.read(move |index| index.range_metas_from(generation, start)).await?;
        let entries = metas
            .into_iter()
            .filter(|meta| meta.state != VerifyState::Rejected)
            .map(|meta| Ok(ExportedRange { entry: meta.entry, state: meta.state }.into()))
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
                let listed = index
                    .range_meta(start)?
                    .is_some_and(|meta| meta.state != VerifyState::Rejected);
                if !listed {
                    return Ok(None);
                }
                index.read_sealed_range_bytes_at(generation, start)
            })
            .await?
            .map(Bytes::from)
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
            Ok(RangeChunk { offset: at as u64, data: blob.slice(at..end) })
        });
        Ok(Response::new(Box::pin(tokio_stream::iter(chunks))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::fixtures;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{
        Code,
        transport::{Channel, Server},
    };
    use ztreamer_indexer::index::range_meta::{
        BlobDigest, Frontiers, ManifestEntry as Entry, RangeMeta,
    };
    use ztreamer_protocol::replicate::{
        replicator_client::ReplicatorClient, replicator_server::ReplicatorServer,
    };

    /// Five sealed ranges. Ranges 0, 1,000 and 2,000 have rows, range 3,000 has none, and the
    /// row of range 4,000 is rejected.
    struct Fixture {
        index: Arc<Index>,
        service: ReplicatorService,
        generation: u64,
        snapshots: watch::Sender<ServingSnapshot>,
        _dir: tempfile::TempDir,
    }

    impl Fixture {
        fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let network = Network::Mainnet;
            let index = Arc::new(
                Index::open(dir.path(), 512 * 1024 * 1024, "Mainnet", network.genesis_hash())
                    .unwrap(),
            );
            let records = fixtures::mixed_records(5_010);
            let state = fixtures::populate(&index, &records, Some(4_999));
            for (start, state) in [
                (0, VerifyState::Verified),
                (1_000, VerifyState::Verified),
                (2_000, VerifyState::Unverified),
                (4_000, VerifyState::Rejected),
            ] {
                let blob = index.read_sealed_range_bytes(start).unwrap().unwrap();
                let last = &records[start as usize + 999];
                let entry = Entry {
                    range_version: RANGE_FORMAT_VERSION,
                    start_height: start,
                    end_height: last.height,
                    end_hash: last.hash,
                    end_tree_sizes: last.end_tree_sizes,
                    frontiers: Frontiers { sapling: vec![0xab; 40], ..Frontiers::default() },
                    subtree_roots: Vec::new(),
                    blob_digest: BlobDigest::hash(&blob),
                    blob_len: blob.len() as u64,
                    epoch_end: start == 0,
                };
                let meta = RangeMeta {
                    entry,
                    state,
                    verified_by: None,
                    verified_at: None,
                    attested: None,
                    sampled_heights: Vec::new(),
                };
                index.put_range_meta(start, &meta).unwrap();
            }
            let (snapshots, receiver) = watch::channel(ServingSnapshot::from(state));
            let service = ReplicatorService::new(Arc::clone(&index), receiver, &network);
            Self { index, service, generation: state.generation(), snapshots, _dir: dir }
        }

        /// Serves the fixture on a free local port and connects a client to it.
        async fn client(&self) -> ReplicatorClient<Channel> {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let server = Server::builder().add_service(ReplicatorServer::new(self.service.clone()));
            tokio::spawn(server.serve_with_incoming(TcpListenerStream::new(listener)));
            ReplicatorClient::connect(endpoint).await.unwrap()
        }
    }

    async fn range_bytes(
        client: &mut ReplicatorClient<Channel>,
        start: u32,
        offset: u64,
    ) -> Vec<u8> {
        let mut stream = client
            .get_range(GetRangeRequest { start_height: start, offset })
            .await
            .unwrap()
            .into_inner();
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.message().await.unwrap() {
            assert_eq!(chunk.offset, offset + bytes.len() as u64);
            assert!(chunk.data.len() <= CHUNK_BYTES);
            bytes.extend_from_slice(&chunk.data);
        }
        bytes
    }

    #[tokio::test]
    async fn export_info_describes_the_index() {
        let fixture = Fixture::new();
        let info = fixture.client().await.get_export_info(Empty {}).await.unwrap().into_inner();
        assert_eq!(
            info,
            ExportInfo {
                network: "Mainnet".to_owned(),
                genesis_hash: Network::Mainnet.genesis_hash().0.to_vec(),
                range_versions: vec![1],
                manifest_version: 1,
                sealed_through: Some(4_999),
                verified_through: None,
                generation: fixture.generation,
            }
        );
    }

    #[tokio::test]
    async fn list_ranges_starts_at_the_given_height_and_skips_unlisted_ranges() {
        let fixture = Fixture::new();
        let mut client = fixture.client().await;
        let mut stream = client
            .list_ranges(ListRangesRequest { from_start_height: 1_000 })
            .await
            .unwrap()
            .into_inner();
        let mut listed = Vec::new();
        while let Some(entry) = stream.message().await.unwrap() {
            listed.push(ExportedRange::try_from(entry).unwrap());
        }
        let expected = [1_000, 2_000].map(|start| {
            let meta = fixture.index.range_meta(start).unwrap().unwrap();
            ExportedRange { entry: meta.entry, state: meta.state }
        });
        assert_eq!(listed, expected);
    }

    #[tokio::test]
    async fn get_range_sends_the_blob_and_resumes_from_an_offset() {
        let fixture = Fixture::new();
        let mut client = fixture.client().await;
        let blob = fixture.index.read_sealed_range_bytes(1_000).unwrap().unwrap();
        assert_eq!(range_bytes(&mut client, 1_000, 0).await, blob);
        let offset = blob.len() as u64 / 3;
        assert_eq!(range_bytes(&mut client, 1_000, offset).await, blob[offset as usize..]);
        assert!(range_bytes(&mut client, 1_000, blob.len() as u64).await.is_empty());

        let refused = |start, offset| {
            let mut client = client.clone();
            async move {
                client
                    .get_range(GetRangeRequest { start_height: start, offset })
                    .await
                    .unwrap_err()
                    .code()
            }
        };
        assert_eq!(refused(1_000, blob.len() as u64 + 1).await, Code::OutOfRange);
        // No row, a rejected row, not a range start, and not sealed.
        for start in [3_000, 4_000, 1_001, 5_000] {
            assert_eq!(refused(start, 0).await, Code::NotFound);
        }
    }

    /// The cap counts streams until they are dropped. The responses are held here, unread, so
    /// the test does not depend on how fast a transport drains a small blob.
    #[tokio::test]
    async fn the_fifth_concurrent_stream_is_refused() {
        let fixture = Fixture::new();
        let request = || Request::new(GetRangeRequest { start_height: 0, offset: 0 });
        let mut streams = Vec::new();
        for _ in 0..MAX_RANGE_STREAMS {
            streams.push(fixture.service.get_range(request()).await.unwrap());
        }
        let refused = fixture.service.get_range(request()).await.err().unwrap();
        assert_eq!(refused.code(), Code::ResourceExhausted);
        drop(streams.pop());
        fixture.service.get_range(request()).await.unwrap();
    }

    #[tokio::test]
    async fn a_moved_generation_is_unavailable() {
        let fixture = Fixture::new();
        let mut client = fixture.client().await;
        fixture.snapshots.send_modify(|snapshot| snapshot.generation += 1);
        let status = client.get_export_info(Empty {}).await.unwrap_err();
        assert_eq!(status.code(), Code::Unavailable);
        let status =
            client.get_range(GetRangeRequest { start_height: 0, offset: 0 }).await.unwrap_err();
        assert_eq!(status.code(), Code::Unavailable);
    }
}
