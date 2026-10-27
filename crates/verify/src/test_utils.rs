//! Test fixtures: an index with `range_meta` rows, its snapshot directory, an HTTP server for
//! that directory, and a Replicator gRPC server for the index.
//!
//! Compiled for this crate's own tests and for other crates that enable the `test-utils`
//! feature in their `[dev-dependencies]`.

use std::{
    convert::Infallible,
    io,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use chrono::{DateTime, TimeDelta, Utc};
use ed25519_dalek::SigningKey;
use futures_util::stream;
use http::{Request, Response, StatusCode, Uri, header::RANGE};
use http_body_util::{BodyExt as _, Full, StreamBody, combinators::BoxBody};
use hyper::{body::Frame, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use tempfile::TempDir;
use tokio::{
    net::TcpListener,
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch},
};
use tokio_stream::{
    Stream,
    wrappers::{ReceiverStream, TcpListenerStream},
};
use tonic::{Status, transport::Server};
use zakura_chain::{block, parameters::Network};
use ztreamer_indexer::{
    codec::{CompactBlockRecord, TreeSizes},
    index::{
        Index, IndexState, RANGE_SIZE,
        range_meta::{BlobDigest, ManifestEntry},
    },
    parser::parse_block,
    test_utils::{populate, raw_block},
    types::RawIndexBlock,
};
use ztreamer_protocol::replicate::{
    Empty, ExportInfo, GetRangeRequest, ListRangesRequest, RangeChunk,
    replicator_server::{Replicator, ReplicatorServer},
};
use ztreamer_service::{InProcessBackend, RangeMetaFiller, ReplicatorService, ServingSnapshot};

use crate::{
    manifest::Manifest,
    snapshot::{RangePath, SnapshotWriter},
};

/// The seed of the key that signs a [`Snapshot`].
pub const SEED: [u8; 32] = [7; 32];

/// A Mainnet index in a temporary directory, sealed through a height, with or without a
/// `range_meta` row for each sealed range.
#[derive(Debug)]
pub struct SourceIndex {
    /// The index.
    pub index: Arc<Index>,
    /// The state of the index after the last write.
    pub state: IndexState,
    dir: TempDir,
}

impl SourceIndex {
    /// An index with `ranges` sealed ranges of empty blocks, ten mutable blocks, and a
    /// `range_meta` row for each sealed range from [`RangeMetaFiller`].
    pub async fn new(ranges: u32) -> Self {
        let records = Self::records(ranges * RANGE_SIZE + 10);
        Self::with_range_meta(&records, ranges * RANGE_SIZE - 1).await
    }

    /// An index with `records`, sealed through `sealed_through`, and with a `range_meta` row for
    /// each sealed range from [`RangeMetaFiller`].
    pub async fn with_range_meta(records: &[CompactBlockRecord], sealed_through: u32) -> Self {
        let source = Self::sealed(records, sealed_through);
        let (_state, read, _tip, _change) = zakura_state::init(
            zakura_state::Config::ephemeral(),
            &Network::Mainnet,
            block::Height::MAX,
            0,
        )
        .await
        .unwrap();
        let (sender, snapshots) = watch::channel(ServingSnapshot::from(source.state));
        drop(sender);
        let backend = Arc::new(InProcessBackend::new(read));
        RangeMetaFiller::embedded(Arc::clone(&source.index), backend).run(snapshots).await;
        source
    }

    /// An index with `records`, sealed through `sealed_through`, with no `range_meta` row.
    pub fn sealed(records: &[CompactBlockRecord], sealed_through: u32) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let index = Arc::new(Self::open(&dir));
        let state = populate(&index, records, Some(sealed_through));
        Self { index, state, dir }
    }

    /// Closes the index and gives its directory. Another user of the index makes this panic.
    pub fn into_dir(self) -> TempDir {
        drop(Arc::into_inner(self.index).expect("no other user holds the index"));
        self.dir
    }

    /// An empty Mainnet index in `dir`.
    pub fn open(dir: &TempDir) -> Index {
        let network = Network::Mainnet;
        Index::open(dir.path(), 1 << 30, &network.to_string(), network.genesis_hash()).unwrap()
    }

    /// The records of [`raw_block`] from genesis, as the parser gives them.
    pub fn records(count: u32) -> Vec<CompactBlockRecord> {
        Self::parse_records((0..count).map(raw_block))
    }

    /// The records of `blocks` from genesis, as the parser gives them.
    pub fn parse_records(
        blocks: impl IntoIterator<Item = RawIndexBlock>,
    ) -> Vec<CompactBlockRecord> {
        let mut sizes = TreeSizes::default();
        blocks
            .into_iter()
            .map(|block| {
                let record = parse_block(&block).unwrap().into_record(sizes).unwrap();
                sizes = record.end_tree_sizes;
                record
            })
            .collect()
    }

    /// The entries of the rows from genesis to the sealed tip, with `epoch_end` on the last
    /// entry, as `ztreamerd export-snapshot` gives them to [`SnapshotWriter`].
    pub fn entries(&self) -> Vec<ManifestEntry> {
        let ranges = self.state.sealed_through().unwrap() / RANGE_SIZE + 1;
        let mut entries: Vec<_> = (0..ranges)
            .map(|range| self.index.range_meta(range * RANGE_SIZE).unwrap().unwrap().entry)
            .collect();
        entries.last_mut().unwrap().epoch_end = true;
        entries
    }
}

/// A snapshot directory that [`SnapshotWriter`] wrote, signed with [`SEED`].
#[derive(Debug)]
pub struct Snapshot {
    /// The last manifest.
    pub manifest: Manifest,
    /// The path of the manifest that `latest.json` names, relative to [`Self::dir`].
    pub manifest_path: String,
    /// The time of the last manifest. Each new manifest is one day after the last manifest.
    published_at: DateTime<Utc>,
    /// The snapshot directory.
    pub dir: TempDir,
}

impl Snapshot {
    /// Exports the rows of `source` from genesis to the sealed tip at 2026-10-26T00:00:00Z.
    pub fn export(source: &SourceIndex) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let manifest =
            Manifest::new("Mainnet", Network::Mainnet.genesis_hash(), source.entries()).unwrap();
        let published_at = DateTime::from_timestamp(1_792_972_800, 0).unwrap();
        let exported = Self::writer(&dir).export(&source.index, &manifest, published_at).unwrap();
        Self { manifest, manifest_path: exported.manifest, published_at, dir }
    }

    /// Writes `blob` as the blob of the range at `start`. Then it writes a new manifest with the
    /// entry of `blob`, as a source that sends a wrong range does.
    pub fn replace_blob(&mut self, start: u32, blob: &[u8]) {
        let mut entries = self.manifest.entries().to_vec();
        let entry = &mut entries[(start / RANGE_SIZE) as usize];
        entry.blob_digest = BlobDigest::hash(blob);
        entry.blob_len = blob.len() as u64;
        Self::writer(&self.dir).put_range(entry, blob).unwrap();
        self.publish(entries);
    }

    /// The file of the blob of `entry`.
    pub fn range_path(&self, entry: &ManifestEntry) -> PathBuf {
        self.dir.path().join(RangePath::from(entry).as_str())
    }

    /// Writes `entries` as a new manifest, one day after the last manifest.
    fn publish(&mut self, entries: Vec<ManifestEntry>) {
        self.manifest = Manifest::new("Mainnet", Network::Mainnet.genesis_hash(), entries).unwrap();
        self.published_at += TimeDelta::days(1);
        let writer = Self::writer(&self.dir);
        let path = writer.new_manifest_path(self.published_at).unwrap();
        writer.put_manifest_and_latest(&path, &self.manifest).unwrap();
        self.manifest_path = path;
    }

    fn writer(dir: &TempDir) -> SnapshotWriter {
        SnapshotWriter::new(dir.path(), SigningKey::from_bytes(&SEED))
    }
}

/// The requests that a [`FileServer`] got: the path and the `Range` header.
type Requests = Vec<(String, Option<String>)>;

/// An HTTP/1 server for the files below a directory. It answers a `Range: bytes=<offset>-`
/// request with 206 and the bytes from the offset.
#[derive(Debug)]
pub struct FileServer {
    /// The base URL of the directory.
    pub base: Uri,
    state: Arc<Served>,
}

#[derive(Debug)]
struct Served {
    root: PathBuf,
    /// The next answer for this path sends half of its bytes, then stops with an error.
    cut: Mutex<Option<String>>,
    requests: Mutex<Requests>,
}

type Body = BoxBody<Bytes, io::Error>;

impl FileServer {
    /// Serves the files below `root` on a free local port.
    pub async fn start(root: &Path) -> Self {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap()).parse().unwrap();
        let state = Arc::new(Served {
            root: root.to_owned(),
            cut: Mutex::default(),
            requests: Mutex::default(),
        });
        let served = Arc::clone(&state);
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let served = Arc::clone(&served);
                let service = service_fn(move |request| Arc::clone(&served).answer(request));
                tokio::spawn(http1::Builder::new().serve_connection(TokioIo::new(stream), service));
            }
        });
        Self { base, state }
    }

    /// Cuts the next answer for `path` after half of its bytes.
    pub fn cut_next(&self, path: &str) {
        *self.state.cut.lock().unwrap() = Some(path.to_owned());
    }

    /// The requests that the server got, in order.
    pub fn requests(&self) -> Requests {
        self.state.requests.lock().unwrap().clone()
    }
}

impl Served {
    async fn answer<B>(self: Arc<Self>, request: Request<B>) -> Result<Response<Body>, Infallible> {
        let path = request.uri().path().trim_start_matches('/').to_owned();
        let range = request.headers().get(RANGE).map(|value| value.to_str().unwrap().to_owned());
        self.requests.lock().unwrap().push((path.clone(), range.clone()));
        let Ok(bytes) = tokio::fs::read(self.root.join(&path)).await else {
            let empty = Full::default().map_err(io::Error::other).boxed();
            return Ok(Self::response(StatusCode::NOT_FOUND, empty));
        };
        let offset = range.as_deref().map_or(0, |range| {
            range.strip_prefix("bytes=").unwrap().strip_suffix('-').unwrap().parse().unwrap()
        });
        let bytes = Bytes::from(bytes).slice(offset..);
        let status = if range.is_some() { StatusCode::PARTIAL_CONTENT } else { StatusCode::OK };
        let cut = self.cut.lock().unwrap().take_if(|cut| *cut == path).is_some();
        let body = if cut {
            // Fail after the first half is on the wire, not before hyper flushes it.
            let (sender, frames) = mpsc::channel(2);
            let half = bytes.slice(..bytes.len() / 2);
            tokio::spawn(async move {
                sender.send(Ok(Frame::data(half))).await.unwrap();
                tokio::time::sleep(Duration::from_millis(100)).await;
                let _ = sender.send(Err(io::Error::other("transfer cut"))).await;
            });
            StreamBody::new(ReceiverStream::new(frames)).boxed()
        } else {
            Full::new(bytes).map_err(io::Error::other).boxed()
        };
        Ok(Self::response(status, body))
    }

    fn response(status: StatusCode, body: Body) -> Response<Body> {
        let mut response = Response::new(body);
        *response.status_mut() = status;
        response
    }
}

/// The `GetRange` requests that a [`PeerServer`] accepted: the start height and the offset.
type RangeRequests = Vec<(u32, u64)>;

/// A Replicator gRPC server for a [`SourceIndex`] with its own limit of open `GetRange` streams.
///
/// It refuses a stream above the limit with `RESOURCE_EXHAUSTED`, as the real service does. It
/// waits `Self::ANSWER_WAIT` before it answers a stream that it accepts, thus the streams of
/// concurrent fetches overlap. A stream releases its place when it sends its last chunk, thus
/// before the client sees the end. `GetExportInfo` and `ListRanges` come from
/// [`ReplicatorService`].
#[derive(Debug)]
pub struct PeerServer {
    /// The URL of the gRPC endpoint.
    pub endpoint: Uri,
    peer: Arc<Peer>,
}

#[derive(Debug)]
struct Peer {
    service: ReplicatorService,
    index: Arc<Index>,
    stream_places: Arc<Semaphore>,
    /// The streams that are open now, and the most that were open at one time.
    open_streams: Arc<Mutex<(usize, usize)>>,
    refusals: AtomicUsize,
    /// The next stream for this range sends half of its bytes, then stops with an error. It
    /// keeps its place for [`PeerServer::CUT_PLACE_HOLD`] after the cut.
    cut: Mutex<Option<u32>>,
    requests: Mutex<RangeRequests>,
}

impl PeerServer {
    const CHUNK_BYTES: usize = 4 << 10;
    const ANSWER_WAIT: Duration = Duration::from_millis(20);
    const CUT_PLACE_HOLD: Duration = Duration::from_millis(150);

    /// Serves `source` with at most `stream_limit` open `GetRange` streams.
    pub async fn start(source: &SourceIndex, stream_limit: usize) -> Self {
        let (_sender, snapshots) = watch::channel(ServingSnapshot::from(source.state));
        let service =
            ReplicatorService::new(Arc::clone(&source.index), snapshots, &Network::Mainnet);
        let peer = Arc::new(Peer {
            service,
            index: Arc::clone(&source.index),
            stream_places: Arc::new(Semaphore::new(stream_limit)),
            open_streams: Arc::default(),
            refusals: AtomicUsize::new(0),
            cut: Mutex::default(),
            requests: Mutex::default(),
        });
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap()).parse().unwrap();
        let server = Server::builder().add_service(ReplicatorServer::from_arc(Arc::clone(&peer)));
        tokio::spawn(server.serve_with_incoming(TcpListenerStream::new(listener)));
        Self { endpoint, peer }
    }

    /// Cuts the next stream of the range at `start` after half of its bytes.
    pub fn cut_next(&self, start: u32) {
        *self.peer.cut.lock().unwrap() = Some(start);
    }

    /// The `GetRange` requests that the server accepted, in order.
    pub fn requests(&self) -> RangeRequests {
        self.peer.requests.lock().unwrap().clone()
    }

    /// The `GetRange` requests that the server refused.
    pub fn refusals(&self) -> usize {
        self.peer.refusals.load(Ordering::Relaxed)
    }

    /// The most `GetRange` streams that were open at one time.
    pub fn most_open_streams(&self) -> usize {
        self.peer.open_streams.lock().unwrap().1
    }
}

/// The place of one open stream on a [`PeerServer`]. The drop releases it.
struct OpenStream {
    open_streams: Arc<Mutex<(usize, usize)>>,
    _place: OwnedSemaphorePermit,
}

impl OpenStream {
    fn new(open_streams: &Arc<Mutex<(usize, usize)>>, place: OwnedSemaphorePermit) -> Self {
        let mut counts = open_streams.lock().unwrap();
        counts.0 += 1;
        counts.1 = counts.1.max(counts.0);
        drop(counts);
        Self { open_streams: Arc::clone(open_streams), _place: place }
    }
}

impl Drop for OpenStream {
    fn drop(&mut self) {
        self.open_streams.lock().unwrap().0 -= 1;
    }
}

/// The state of one `GetRange` stream of a [`PeerServer`].
struct RangeStream {
    blob: Bytes,
    /// The offset of the next chunk.
    at: usize,
    /// The offset where the stream stops with an error, if it is cut.
    cut_at: Option<usize>,
    /// `None` after the last chunk.
    open: Option<OpenStream>,
}

impl RangeStream {
    async fn next_chunk(mut self) -> Option<(Result<RangeChunk, Status>, Self)> {
        let open = self.open.take()?;
        if self.cut_at == Some(self.at) {
            tokio::spawn(async move {
                tokio::time::sleep(PeerServer::CUT_PLACE_HOLD).await;
                drop(open);
            });
            return Some((Err(Status::aborted("transfer cut")), self));
        }
        if self.at == self.blob.len() {
            return None;
        }
        let stop = self.cut_at.unwrap_or(self.blob.len());
        let end = stop.min(self.at + PeerServer::CHUNK_BYTES);
        let chunk = RangeChunk { data: self.blob.slice(self.at..end) };
        self.at = end;
        self.open = Some(open);
        Some((Ok(chunk), self))
    }
}

type PeerStream<T> = std::pin::Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

#[tonic::async_trait]
impl Replicator for Peer {
    type ListRangesStream = <ReplicatorService as Replicator>::ListRangesStream;
    type GetRangeStream = PeerStream<RangeChunk>;

    async fn get_export_info(
        &self,
        request: tonic::Request<Empty>,
    ) -> Result<tonic::Response<ExportInfo>, Status> {
        self.service.get_export_info(request).await
    }

    async fn list_ranges(
        &self,
        request: tonic::Request<ListRangesRequest>,
    ) -> Result<tonic::Response<Self::ListRangesStream>, Status> {
        self.service.list_ranges(request).await
    }

    async fn get_range(
        &self,
        request: tonic::Request<GetRangeRequest>,
    ) -> Result<tonic::Response<Self::GetRangeStream>, Status> {
        let GetRangeRequest { start_height, offset } = request.into_inner();
        let Ok(place) = Arc::clone(&self.stream_places).try_acquire_owned() else {
            self.refusals.fetch_add(1, Ordering::Relaxed);
            return Err(Status::resource_exhausted("the range streams are running"));
        };
        let open = OpenStream::new(&self.open_streams, place);
        self.requests.lock().unwrap().push((start_height, offset));
        let blob = Bytes::from(self.index.read_sealed_range_bytes(start_height).unwrap().unwrap());
        let at = usize::try_from(offset).unwrap();
        let cut = self.cut.lock().unwrap().take_if(|cut| *cut == start_height).is_some();
        let cut_at = cut.then(|| at + (blob.len() - at) / 2);
        let range_stream = RangeStream { blob, at, cut_at, open: Some(open) };
        tokio::time::sleep(PeerServer::ANSWER_WAIT).await;
        Ok(tonic::Response::new(Box::pin(stream::unfold(range_stream, RangeStream::next_chunk))))
    }
}
