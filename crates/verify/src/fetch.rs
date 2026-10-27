//! Sources of sealed ranges: a snapshot over HTTP, or a peer over the Replicator gRPC service.
//!
//! A [`RangeSource`] gives the [`Manifest`] of its export, and the bytes of one range blob from
//! a byte offset. The two implementations are:
//!
//! - [`HttpSource`] reads a snapshot directory that `ztreamerd export-snapshot` wrote, below a
//!   base URL, in the layout of [`crate::snapshot`]. It reads [`Latest`], then the manifest
//!   that it names, and verifies the Ed25519 signature of each with a key that the caller
//!   gives. It does not use the key in `latest.json`: a source that can change the files can
//!   also change that key. It reads a blob from its [`RangePath`] with an HTTP `Range` request.
//! - [`PeerSource`] asks a ztreamer that serves the Replicator gRPC service. It builds the
//!   manifest from `ListRanges`, up to the last listed range that closes an epoch. It opens at
//!   most [`PEER_RANGE_STREAMS`] `GetRange` streams at a time. When the peer refuses a stream
//!   with `RESOURCE_EXHAUSTED`, it waits and asks again: a busy peer is not a failed peer.
//!
//! [`Fetcher`] reads complete blobs from a source. It continues a cut transfer from the last
//! byte that it received, and it checks the length and the BLAKE3 digest of each blob against
//! its manifest entry. A blob that fails this check is [`FetchError::Digest`].

use std::{fmt, pin::Pin, sync::Arc, time::Duration};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use ed25519_dalek::{Signature, VerifyingKey};
use http::{Request, StatusCode, Uri, header::RANGE};
use http_body_util::{BodyExt as _, Empty};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::TokioExecutor,
};
use tokio::sync::Semaphore;
use tokio_stream::{Stream, StreamExt as _};
use tonic::{Code, Streaming, transport::Channel};
use tracing::{debug, warn};
use zakura_chain::block;
use ztreamer_indexer::index::range_meta::{ExportedRange, ManifestEntry, WireError};
use ztreamer_protocol::replicate::{
    self, GetRangeRequest, ListRangesRequest, RangeChunk, replicator_client::ReplicatorClient,
};

use crate::{
    manifest::{MANIFEST_VERSION, Manifest, ManifestError},
    range::RangeBlob,
    snapshot::{Latest, LatestError, RangePath, SIGNATURE_SUFFIX},
};

/// The times that [`Fetcher::blob`] continues one blob after a transfer error.
pub const MAX_RESUMES: u32 = 3;

/// The `GetRange` streams that one [`PeerSource`] opens at a time. It is the stream limit of the
/// Replicator service of ztreamer (`MAX_RANGE_STREAMS`). A peer with a lower limit refuses the
/// streams above it, and [`PeerSource`] asks again.
pub const PEER_RANGE_STREAMS: usize = 4;

/// The bytes of a range blob, in order, from the offset of the request.
pub type BlobStream = Pin<Box<dyn Stream<Item = Result<Bytes, FetchError>> + Send>>;

/// Why a source gave no manifest or no blob.
#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    /// The base URL and a path do not make a URL.
    #[error("{0} is not a valid URL")]
    Url(String),
    /// The HTTP request failed.
    #[error("HTTP request for {path} failed")]
    Http {
        /// The path below the base URL.
        path: String,
        /// Why the request failed.
        #[source]
        source: hyper_util::client::legacy::Error,
    },
    /// The HTTP body stopped with an error.
    #[error("HTTP body failed")]
    Body(#[from] hyper::Error),
    /// The HTTP server answered with another status.
    #[error("HTTP status {status} for {path}")]
    Status {
        /// The path below the base URL.
        path: String,
        /// The status of the answer.
        status: StatusCode,
    },
    /// The TLS setup failed.
    #[error("TLS setup failed")]
    Tls(#[from] rustls::Error),
    /// The gRPC connection failed.
    #[error("gRPC connection failed")]
    Connect(#[from] tonic::transport::Error),
    /// A gRPC call failed.
    #[error("gRPC call failed")]
    Grpc(#[from] tonic::Status),
    /// `latest.json` does not parse, has no signature, or its signature does not verify with
    /// the key of the caller.
    #[error(transparent)]
    Latest(#[from] LatestError),
    /// `latest.json` and the manifest that it names give another sealed height.
    #[error("latest.json is sealed through {latest}, its manifest through {manifest}")]
    LatestHeight {
        /// The sealed height in `latest.json`.
        latest: u32,
        /// The sealed height of the manifest.
        manifest: u32,
    },
    /// The manifest is not UTF-8.
    #[error("manifest is not UTF-8")]
    Utf8(#[from] std::str::Utf8Error),
    /// The manifest does not parse or does not obey the manifest rules.
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    /// A Replicator message does not convert.
    #[error(transparent)]
    Wire(#[from] WireError),
    /// The peer sends another manifest version.
    #[error("peer sends manifest version {0}")]
    Version(u32),
    /// The signature of the manifest is not hex, or does not verify with the key of the caller.
    #[error("signature of {path} does not verify")]
    Signature {
        /// The path of the signed file below the base URL.
        path: String,
    },
    /// The length or the BLAKE3 digest of a blob is not the one of its manifest entry.
    #[error("blob of range {start_height} does not match its manifest digest or length")]
    Digest {
        /// The first height of the range.
        start_height: u32,
    },
}

impl FetchError {
    /// Returns `true` for an error of the transfer, which a new request can avoid.
    const fn is_transfer(&self) -> bool {
        matches!(self, Self::Http { .. } | Self::Body(_) | Self::Grpc(_))
    }
}

/// A source of sealed ranges.
///
/// # Implementation
///
/// Give the manifest only after its checks pass: a [`Manifest`] is always valid, and a signed
/// source must verify its signatures first. [`RangeSource::fetch_blob`] sends the bytes of the
/// blob from `offset` to its end, and an error when the transfer stops early. It does not check
/// the digest: [`Fetcher::blob`] does. [`Fetcher`] and the importer hold a source as a
/// `Box<dyn RangeSource>`. The `Debug` form names the source, for logs.
#[async_trait]
pub trait RangeSource: fmt::Debug + Send + Sync {
    /// The manifest of the source.
    async fn manifest(&self) -> Result<Manifest, FetchError>;

    /// The bytes of the blob of `entry` from byte `offset`.
    async fn fetch_blob(
        &self,
        entry: &ManifestEntry,
        offset: u64,
    ) -> Result<BlobStream, FetchError>;
}

/// Reads complete blobs from a source, and checks the length and the digest of each blob.
///
/// ```no_run
/// use ed25519_dalek::VerifyingKey;
/// use ztreamer_verify::fetch::{FetchError, Fetcher, HttpSource};
///
/// # async fn run(key: VerifyingKey) -> Result<(), FetchError> {
/// let source = HttpSource::new("https://snapshots.example/mainnet".parse().unwrap(), key)?;
/// let fetcher = Fetcher::new(Box::new(source));
/// let manifest = fetcher.manifest().await?;
/// let blob = fetcher.blob(&manifest.entries()[0]).await?;
/// # let _ = blob;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Fetcher {
    source: Box<dyn RangeSource>,
}

impl Fetcher {
    /// A fetcher of the blobs of `source`.
    pub fn new(source: Box<dyn RangeSource>) -> Self {
        Self { source }
    }

    /// The manifest of the source.
    pub async fn manifest(&self) -> Result<Manifest, FetchError> {
        self.source.manifest().await
    }

    /// The blob of `entry`, with the length and the digest of the entry.
    ///
    /// A request or a transfer that fails continues from the last byte received, at most
    /// [`MAX_RESUMES`] times. A blob with a wrong length or digest is [`FetchError::Digest`].
    /// The stream of a failed transfer is dropped before the next request, thus the source can
    /// release its place before the resume.
    pub async fn blob(&self, entry: &ManifestEntry) -> Result<Bytes, FetchError> {
        let mut blob = BytesMut::new();
        let mut resumes = 0;
        while let Err(error) = self.transfer(entry, &mut blob).await {
            if !error.is_transfer() || resumes == MAX_RESUMES {
                return Err(error);
            }
            resumes += 1;
            let (start_height, offset) = (entry.start_height, blob.len());
            warn!(%error, start_height, offset, "range transfer stopped; resuming");
        }
        if RangeBlob::new(&blob, entry).matches_digest() {
            return Ok(blob.freeze());
        }
        Err(FetchError::Digest { start_height: entry.start_height })
    }

    /// Appends the bytes of the blob of `entry` from `blob.len()` to `blob`, with one request.
    async fn transfer(&self, entry: &ManifestEntry, blob: &mut BytesMut) -> Result<(), FetchError> {
        let mut chunks = self.source.fetch_blob(entry, blob.len() as u64).await?;
        // More bytes than the entry gives fail the digest check: stop reading.
        while blob.len() as u64 <= entry.blob_len {
            let Some(chunk) = chunks.next().await else {
                break;
            };
            blob.extend_from_slice(&chunk?);
        }
        Ok(())
    }
}

/// The HTTP client of [`HttpSource`].
type HttpClient = Client<HttpsConnector<HttpConnector>, Empty<Bytes>>;

/// A snapshot directory below a base URL, signed with a key that the caller trusts.
#[derive(Clone)]
pub struct HttpSource {
    base: String,
    key: VerifyingKey,
    client: HttpClient,
}

impl fmt::Debug for HttpSource {
    /// The base URL only. The client holds no data for a log.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpSource").field("base", &self.base).finish_non_exhaustive()
    }
}

impl HttpSource {
    /// A source below `base` whose files `key` signs. It does not contact the server.
    pub fn new(base: Uri, key: VerifyingKey) -> Result<Self, FetchError> {
        // A copy of the connector of `ztreamer_remote::RpcClient`, which is private, with the
        // web PKI roots only.
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_no_client_auth();
        let mut tcp = HttpConnector::new();
        tcp.enforce_http(false);
        tcp.set_nodelay(true);
        let https = HttpsConnectorBuilder::new()
            .with_tls_config(tls)
            .https_or_http()
            .enable_http1()
            .wrap_connector(tcp);
        let client = Client::builder(TokioExecutor::new()).build(https);
        Ok(Self { base: base.to_string().trim_end_matches('/').to_owned(), key, client })
    }

    /// Sends a GET for `path` below the base URL, from byte `offset`, and checks the status.
    async fn get(
        &self,
        path: &str,
        offset: u64,
    ) -> Result<http::Response<hyper::body::Incoming>, FetchError> {
        let url = format!("{}/{path}", self.base);
        let uri = url.parse::<Uri>().map_err(|_| FetchError::Url(url))?;
        let mut request = Request::get(uri);
        if offset > 0 {
            request = request.header(RANGE, format!("bytes={offset}-"));
        }
        let request = request.body(Empty::new()).expect("a GET with a parsed URI builds");
        let response = self
            .client
            .request(request)
            .await
            .map_err(|source| FetchError::Http { path: path.to_owned(), source })?;
        let expected = if offset > 0 { StatusCode::PARTIAL_CONTENT } else { StatusCode::OK };
        if response.status() != expected {
            return Err(FetchError::Status { path: path.to_owned(), status: response.status() });
        }
        Ok(response)
    }

    /// Reads the file at `path` to its end.
    async fn read(&self, path: &str) -> Result<Bytes, FetchError> {
        Ok(self.get(path, 0).await?.into_body().collect().await?.to_bytes())
    }

    /// Reads `path` and its signature file, and verifies the signature with the key of the
    /// caller.
    async fn signed(&self, path: &str) -> Result<Bytes, FetchError> {
        let bytes = self.read(path).await?;
        let signature_path = format!("{path}{SIGNATURE_SUFFIX}");
        let hex = self.read(&signature_path).await?;
        let refused = || FetchError::Signature { path: path.to_owned() };
        let signature = hex::decode(hex.trim_ascii()).map_err(|_| refused())?;
        let signature = Signature::from_slice(&signature).map_err(|_| refused())?;
        self.key.verify_strict(&bytes, &signature).map_err(|_| refused())?;
        Ok(bytes)
    }

    /// Reads `latest.json` and verifies its signature with the key of the caller.
    async fn latest(&self) -> Result<Latest, FetchError> {
        Ok(Latest::from_file(&self.read(Latest::PATH).await?, &self.key)?)
    }
}

#[async_trait]
impl RangeSource for HttpSource {
    async fn manifest(&self) -> Result<Manifest, FetchError> {
        let latest = self.latest().await?;
        let bytes = self.signed(&latest.manifest).await?;
        let manifest = Manifest::from_json(std::str::from_utf8(&bytes)?)?;
        if manifest.sealed_through() != latest.sealed_through {
            let (latest, manifest) = (latest.sealed_through, manifest.sealed_through());
            return Err(FetchError::LatestHeight { latest, manifest });
        }
        Ok(manifest)
    }

    async fn fetch_blob(
        &self,
        entry: &ManifestEntry,
        offset: u64,
    ) -> Result<BlobStream, FetchError> {
        let body = self.get(RangePath::from(entry).as_str(), offset).await?.into_body();
        Ok(Box::pin(body.into_data_stream().map(|chunk| chunk.map_err(FetchError::from))))
    }
}

/// The waits before each new `GetRange` request to a peer that refused a stream with
/// `RESOURCE_EXHAUSTED`: [`Self::FIRST`], then double the wait before, up to [`Self::MAX`]. The
/// iterator does not end.
#[derive(Clone, Copy, Debug)]
struct RefusalBackoff {
    next: Duration,
}

impl RefusalBackoff {
    /// The wait after the first refusal.
    const FIRST: Duration = Duration::from_millis(50);
    /// The longest wait.
    const MAX: Duration = Duration::from_secs(2);
}

impl Default for RefusalBackoff {
    fn default() -> Self {
        Self { next: Self::FIRST }
    }
}

impl Iterator for RefusalBackoff {
    type Item = Duration;

    fn next(&mut self) -> Option<Duration> {
        let wait = self.next;
        self.next = (wait * 2).min(Self::MAX);
        Some(wait)
    }
}

/// A ztreamer that serves the Replicator gRPC service.
///
/// The clones of a source share one connection and one limit of [`PEER_RANGE_STREAMS`] open
/// `GetRange` streams.
#[derive(Clone)]
pub struct PeerSource {
    /// The endpoint of the peer, for logs.
    endpoint: Uri,
    client: ReplicatorClient<Channel>,
    /// One permit for each open `GetRange` stream.
    range_streams: Arc<Semaphore>,
}

impl fmt::Debug for PeerSource {
    /// The endpoint only. The client holds no data for a log.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeerSource").field("endpoint", &self.endpoint).finish_non_exhaustive()
    }
}

impl PeerSource {
    /// A source over `channel`, a connection to `endpoint`.
    pub fn new(endpoint: Uri, channel: Channel) -> Self {
        let range_streams = Arc::new(Semaphore::new(PEER_RANGE_STREAMS));
        Self { endpoint, client: ReplicatorClient::new(channel), range_streams }
    }

    /// Connects to the Replicator service at `endpoint`.
    pub async fn connect(endpoint: Uri) -> Result<Self, FetchError> {
        let channel = Channel::builder(endpoint.clone()).connect().await?;
        Ok(Self::new(endpoint, channel))
    }

    /// Opens the `GetRange` stream of the range at `start_height` from byte `offset`.
    ///
    /// A `RESOURCE_EXHAUSTED` refusal is back-pressure: the peer runs its limit of streams. The
    /// request waits with [`RefusalBackoff`] and asks again, with no limit on the attempts. A
    /// stream that stopped can still hold its place on the peer for a short time after the
    /// client drops it, thus a resume can get this refusal too.
    async fn open_range_stream(
        &self,
        start_height: u32,
        offset: u64,
    ) -> Result<Streaming<RangeChunk>, FetchError> {
        let mut waits = RefusalBackoff::default();
        loop {
            let request = GetRangeRequest { start_height, offset };
            match self.client.clone().get_range(request).await {
                Err(status) if status.code() == Code::ResourceExhausted => {
                    let wait = waits.next().expect("the backoff does not end");
                    debug!(start_height, offset, %status, ?wait, "peer is busy; asking again");
                    tokio::time::sleep(wait).await;
                }
                response => return Ok(response?.into_inner()),
            }
        }
    }
}

#[async_trait]
impl RangeSource for PeerSource {
    /// Builds the manifest from the listed ranges: the ranges that follow each other from
    /// genesis, up to the last one that closes an epoch.
    async fn manifest(&self) -> Result<Manifest, FetchError> {
        let mut client = self.client.clone();
        let info = client.get_export_info(replicate::Empty {}).await?.into_inner();
        if info.manifest_version != MANIFEST_VERSION {
            return Err(FetchError::Version(info.manifest_version));
        }
        let genesis_hash = <[u8; 32]>::try_from(info.genesis_hash)
            .map_err(|_| WireError::Length("genesis_hash"))?;
        let mut listed =
            client.list_ranges(ListRangesRequest { from_start_height: 0 }).await?.into_inner();
        let mut entries = Vec::new();
        while let Some(entry) = listed.message().await? {
            let entry = ExportedRange::try_from(entry)?.entry;
            let next = entries.last().map_or(0, |last: &ManifestEntry| last.end_height + 1);
            if entry.start_height != next {
                break;
            }
            entries.push(entry);
        }
        let closed = entries.iter().rposition(|entry| entry.epoch_end).map_or(0, |at| at + 1);
        entries.truncate(closed);
        Ok(Manifest::new(info.network, block::Hash(genesis_hash), entries)?)
    }

    async fn fetch_blob(
        &self,
        entry: &ManifestEntry,
        offset: u64,
    ) -> Result<BlobStream, FetchError> {
        let permit = Arc::clone(&self.range_streams)
            .acquire_owned()
            .await
            .expect("the stream permits are never closed");
        let chunks = self.open_range_stream(entry.start_height, offset).await?;
        // The permit lasts until the blob stream is dropped.
        Ok(Box::pin(chunks.map(move |chunk| {
            let _ = &permit;
            chunk.map(|chunk| chunk.data).map_err(FetchError::from)
        })))
    }
}

#[cfg(test)]
mod tests {
    use ed25519_dalek::SigningKey;
    use futures_util::future;
    use tokio::sync::watch;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;
    use zakura_chain::parameters::Network;
    use ztreamer_indexer::index::RANGE_SIZE;
    use ztreamer_protocol::replicate::replicator_server::ReplicatorServer;
    use ztreamer_service::{ReplicatorService, ServingSnapshot, replicate::MAX_RANGE_STREAMS};

    use super::*;
    use crate::test_utils::{FileServer, PeerServer, SEED, Snapshot, SourceIndex};

    /// Twelve ranges: the filler closes an epoch at range 9 only.
    const RANGES: u32 = 12;

    struct Http {
        source: SourceIndex,
        snapshot: Snapshot,
        server: FileServer,
    }

    impl Http {
        async fn new() -> Self {
            let source = SourceIndex::new(RANGES).await;
            let snapshot = Snapshot::export(&source);
            let server = FileServer::start(snapshot.dir.path()).await;
            Self { source, snapshot, server }
        }

        fn fetcher(&self, seed: [u8; 32]) -> Fetcher {
            let key = SigningKey::from_bytes(&seed).verifying_key();
            Fetcher::new(Box::new(HttpSource::new(self.server.base.clone(), key).unwrap()))
        }

        fn blob(&self, start: u32) -> Vec<u8> {
            self.source.index.read_sealed_range_bytes(start).unwrap().unwrap()
        }
    }

    #[tokio::test]
    async fn http_manifest_verifies_with_the_key_of_the_caller() {
        let http = Http::new().await;
        let fetcher = http.fetcher(SEED);

        assert_eq!(fetcher.manifest().await.unwrap(), http.snapshot.manifest);
        let base = http.server.base.to_string();
        assert!(format!("{fetcher:?}").contains(base.trim_end_matches('/')), "{fetcher:?}");
        let entry = &http.snapshot.manifest.entries()[3];
        assert_eq!(fetcher.blob(entry).await.unwrap(), http.blob(3_000));
    }

    #[tokio::test]
    async fn a_bad_signature_refuses_the_manifest() {
        let http = Http::new().await;
        // `latest.json` holds the key of seed 8. The source does not trust it.
        let error = http.fetcher([8; 32]).manifest().await.unwrap_err();
        assert!(matches!(error, FetchError::Latest(LatestError::Signature)), "{error:?}");

        let other = SigningKey::from_bytes(&[9; 32]);
        let bytes = std::fs::read(http.snapshot.dir.path().join(Snapshot::MANIFEST)).unwrap();
        http.snapshot.write_signed_with(Snapshot::MANIFEST, &bytes, &other);
        let error = http.fetcher(SEED).manifest().await.unwrap_err();
        assert!(
            matches!(&error, FetchError::Signature { path } if path == Snapshot::MANIFEST),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn a_cut_transfer_resumes_from_the_offset() {
        let http = Http::new().await;
        let entry = &http.snapshot.manifest.entries()[1];
        let path = http.snapshot.range_path(entry);
        let relative = path.strip_prefix(http.snapshot.dir.path()).unwrap().to_str().unwrap();
        http.server.cut_next(relative);

        let blob = http.fetcher(SEED).blob(entry).await.unwrap();

        assert_eq!(blob, http.blob(1_000));
        let requests: Vec<_> =
            http.server.requests().into_iter().filter(|(path, _)| path == relative).collect();
        let half = blob.len() / 2;
        assert_eq!(
            requests,
            [(relative.to_owned(), None), (relative.to_owned(), Some(format!("bytes={half}-")))]
        );
    }

    #[tokio::test]
    async fn a_corrupted_blob_is_a_digest_error() {
        let http = Http::new().await;
        let entry = &http.snapshot.manifest.entries()[0];
        let path = http.snapshot.range_path(entry);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[100] ^= 1;
        std::fs::write(&path, bytes).unwrap();
        let fetcher = http.fetcher(SEED);

        let error = fetcher.blob(entry).await.unwrap_err();

        assert!(matches!(error, FetchError::Digest { start_height: 0 }), "{error:?}");
    }

    #[tokio::test]
    async fn peer_manifest_ends_at_the_last_closed_epoch_and_blobs_resume() {
        let source = SourceIndex::new(RANGES).await;
        let (_sender, snapshots) = watch::channel(ServingSnapshot::from(source.state));
        let service =
            ReplicatorService::new(Arc::clone(&source.index), snapshots, &Network::Mainnet);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap()).parse().unwrap();
        let server = Server::builder().add_service(ReplicatorServer::new(service));
        tokio::spawn(server.serve_with_incoming(TcpListenerStream::new(listener)));
        let peer = PeerSource::connect(endpoint).await.unwrap();

        let manifest = peer.manifest().await.unwrap();

        // The rows of ranges 10 and 11 are in no closed epoch.
        let mut expected = source.entries();
        expected.truncate(10);
        assert_eq!(manifest.entries(), expected);
        let entry = &manifest.entries()[2];
        let blob = source.index.read_sealed_range_bytes(2_000).unwrap().unwrap();
        let mut tail = Vec::new();
        let mut chunks = peer.fetch_blob(entry, 100).await.unwrap();
        while let Some(chunk) = chunks.next().await {
            tail.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(tail, blob[100..]);
        assert_eq!(Fetcher::new(Box::new(peer)).blob(entry).await.unwrap(), blob);
    }

    /// A source index and a [`PeerServer`] for it with `stream_limit` streams.
    struct Peer {
        source: SourceIndex,
        server: PeerServer,
        fetcher: Fetcher,
    }

    impl Peer {
        async fn new(stream_limit: usize) -> Self {
            let source = SourceIndex::new(RANGES).await;
            let server = PeerServer::start(&source, stream_limit).await;
            let peer = PeerSource::connect(server.endpoint.clone()).await.unwrap();
            Self { source, server, fetcher: Fetcher::new(Box::new(peer)) }
        }

        /// Fetches the blobs of the first ten ranges at the same time, and checks each one.
        async fn fetch_ten_blobs_at_once(&self) {
            let entries = self.source.entries();
            let blobs = future::try_join_all(entries[..10].iter().map(|e| self.fetcher.blob(e)));
            for (entry, blob) in entries.iter().zip(blobs.await.unwrap()) {
                let expected = self.source.index.read_sealed_range_bytes(entry.start_height);
                assert_eq!(blob, expected.unwrap().unwrap());
            }
        }
    }

    #[test]
    fn the_peer_stream_limit_is_the_stream_limit_of_the_replicator() {
        assert_eq!(PEER_RANGE_STREAMS, MAX_RANGE_STREAMS);
    }

    #[test]
    fn the_refusal_backoff_doubles_up_to_its_maximum() {
        let waits: Vec<_> =
            RefusalBackoff::default().take(8).map(|wait| wait.as_millis()).collect();
        assert_eq!(waits, [50, 100, 200, 400, 800, 1_600, 2_000, 2_000]);
    }

    #[tokio::test]
    async fn a_peer_source_opens_at_most_its_stream_limit() {
        let peer = Peer::new(64).await;

        peer.fetch_ten_blobs_at_once().await;

        assert_eq!(peer.server.most_open_streams(), PEER_RANGE_STREAMS);
        assert_eq!((peer.server.refusals(), peer.server.requests().len()), (0, 10));
    }

    #[tokio::test]
    async fn a_peer_that_refuses_above_two_streams_sends_each_blob_after_a_wait() {
        let peer = Peer::new(2).await;

        peer.fetch_ten_blobs_at_once().await;

        assert!(peer.server.refusals() > 0);
        assert_eq!(peer.server.most_open_streams(), 2);
        let mut requests = peer.server.requests();
        requests.sort_unstable();
        assert_eq!(requests, (0..10).map(|range| (range * RANGE_SIZE, 0)).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn a_cut_peer_stream_resumes_when_the_peer_releases_its_place() {
        // The cut stream keeps the one place of the peer for a time, thus the first resume is
        // refused.
        let peer = Peer::new(1).await;
        let entry = &peer.source.entries()[2];
        peer.server.cut_next(2_000);

        let blob = peer.fetcher.blob(entry).await.unwrap();

        assert_eq!(blob, peer.source.index.read_sealed_range_bytes(2_000).unwrap().unwrap());
        let half = blob.len() as u64 / 2;
        assert_eq!(peer.server.requests(), [(2_000, 0), (2_000, half)]);
        assert!(peer.server.refusals() > 0);
    }
}
