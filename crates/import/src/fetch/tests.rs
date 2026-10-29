use std::sync::Mutex;

use ed25519_dalek::SigningKey;
use futures_util::future;
use tokio::sync::watch;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Status, transport::Server};
use zakura_chain::parameters::Network;
use ztreamer_chain::codec::RANGE_SIZE;
use ztreamer_index::test_utils::sealed_range_blob;
use ztreamer_protocol::replicate::replicator_server::ReplicatorServer;
use ztreamer_sync::head::ServingSnapshot;

use chrono::{DateTime, TimeDelta};
use ztreamer_replicator::{MAX_RANGE_STREAMS, ReplicatorService, snapshot_writer::SnapshotWriter};

use super::*;
use crate::test_utils::{FileServer, PeerServer, SEED, Snapshot, SourceIndex};

/// Twelve ranges: the filler closes an epoch at range 9 only.
const RANGES: u32 = 12;

impl ResumeRule {
    /// Each received byte is progress, and the waits are short. The test blobs are smaller
    /// than [`RESUME_PROGRESS_BYTES`].
    const TEST: Self = Self {
        progress_bytes: 1,
        first_wait: Duration::from_millis(1),
        max_wait_without_bytes: MAX_WAIT_WITHOUT_BYTES,
    };
}

/// A source that sends the first `bytes` bytes of `blob` after the offset of each request,
/// then fails with `status`. It records the offset of each request.
#[derive(Debug)]
struct FailingSource {
    blob: Bytes,
    bytes: usize,
    status: Status,
    offsets: Arc<Mutex<Vec<u64>>>,
}

#[async_trait]
impl RangeSource for FailingSource {
    async fn manifest(&self) -> Result<Manifest, FetchError> {
        unimplemented!("the tests read blobs only")
    }

    async fn fetch_blob(
        &self,
        _entry: &ManifestEntry,
        offset: u64,
    ) -> Result<BlobStream, FetchError> {
        self.offsets.lock().unwrap().push(offset);
        let tail = self.blob.slice(offset as usize..);
        let sent = tail.slice(..self.bytes.min(tail.len()));
        let chunks = [Ok(sent), Err(FetchError::Grpc(self.status.clone()))];
        Ok(Box::pin(tokio_stream::iter(chunks)))
    }
}

/// A source that opens each stream, then sends no byte and does not end. It records the offset
/// of each request.
#[derive(Debug)]
struct SilentSource {
    offsets: Arc<Mutex<Vec<u64>>>,
}

#[async_trait]
impl RangeSource for SilentSource {
    async fn manifest(&self) -> Result<Manifest, FetchError> {
        unimplemented!("the tests read blobs only")
    }

    async fn fetch_blob(
        &self,
        _entry: &ManifestEntry,
        offset: u64,
    ) -> Result<BlobStream, FetchError> {
        self.offsets.lock().unwrap().push(offset);
        Ok(Box::pin(tokio_stream::pending()))
    }
}

struct Http {
    source: SourceIndex,
    snapshot: Snapshot,
    server: FileServer,
}

impl Http {
    async fn new() -> Self {
        Self::with_ranges(RANGES).await
    }

    async fn with_ranges(ranges: u32) -> Self {
        let source = SourceIndex::new(ranges).await;
        let snapshot = Snapshot::export(&source);
        let server = FileServer::start(snapshot.dir.path()).await;
        Self { source, snapshot, server }
    }

    fn fetcher(&self, seed: [u8; 32]) -> Fetcher {
        let key = SigningKey::from_bytes(&seed).verifying_key();
        let source = HttpSource::new(self.server.base.clone(), key).unwrap();
        Fetcher::with_resume_rule(Box::new(source), ResumeRule::TEST)
    }

    fn blob(&self, start: u32) -> Vec<u8> {
        sealed_range_blob(&self.source.index, start)
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
    // `latest.json` holds the key of `SEED`. A source must not trust it.
    let error = http.fetcher([8; 32]).manifest().await.unwrap_err();
    assert!(matches!(error, FetchError::Latest(LatestError::Signature)), "{error:?}");

    let manifest = &http.snapshot.manifest_path;
    let bytes = std::fs::read(http.snapshot.dir.path().join(manifest)).unwrap();
    let other = SignatureFile::sign(&bytes, &SigningKey::from_bytes(&[9; 32]));
    let signature = http.snapshot.dir.path().join(SignatureFile::path_of(manifest));
    std::fs::write(signature, other.to_hex()).unwrap();
    let error = http.fetcher(SEED).manifest().await.unwrap_err();
    assert!(matches!(&error, FetchError::Signature { path } if path == manifest), "{error:?}");
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
async fn a_transfer_that_progresses_resumes_more_than_the_limit_without_progress() {
    let http = Http::new().await;
    let entry = &http.snapshot.manifest.entries()[1];
    let path = http.snapshot.range_path(entry);
    let relative = path.strip_prefix(http.snapshot.dir.path()).unwrap().to_str().unwrap();
    let cuts = MAX_RESUMES_WITHOUT_PROGRESS as usize + 2;
    http.server.cut_next_answers(relative, cuts);

    let blob = http.fetcher(SEED).blob(entry).await.unwrap();

    assert_eq!(blob, http.blob(1_000));
    // Each cut answer sends half of the bytes after its offset.
    let mut offset = 0;
    let mut expected = vec![(relative.to_owned(), None)];
    for _ in 0..cuts {
        offset += (blob.len() - offset) / 2;
        expected.push((relative.to_owned(), Some(format!("bytes={offset}-"))));
    }
    let requests: Vec<_> =
        http.server.requests().into_iter().filter(|(path, _)| path == relative).collect();
    assert_eq!(requests, expected);
}

#[test]
fn stalled_resumes_double_the_wait_and_stop_at_the_limit() {
    let mut stalled = StalledResumes::from(ResumeRule::default());
    let resumes: Vec<_> = (0..6).map(|_| stalled.after(0)).collect();
    let after = |secs| Resume::After(Duration::from_secs(secs));
    assert_eq!(resumes, [after(1), after(2), after(4), after(8), after(16), Resume::Stop]);

    // Fewer than `RESUME_PROGRESS_BYTES` is no progress. More starts the count again.
    let mut stalled = StalledResumes::from(ResumeRule::default());
    let received = [RESUME_PROGRESS_BYTES - 1, 0, RESUME_PROGRESS_BYTES, 0];
    let resumes: Vec<_> = received.into_iter().map(|bytes| stalled.after(bytes)).collect();
    assert_eq!(resumes, [after(1), after(2), Resume::Now, after(1)]);
}

#[test]
fn a_peer_refusal_that_a_new_request_does_not_change_is_no_transfer_error() {
    let refusals = [Status::not_found(""), Status::out_of_range(""), Status::unimplemented("")];
    assert!(refusals.into_iter().all(|status| !FetchError::Grpc(status).is_transfer()));
    let cuts = [Status::aborted(""), Status::unavailable(""), Status::internal("")];
    assert!(cuts.into_iter().all(|status| FetchError::Grpc(status).is_transfer()));
}

/// A fetcher of the blob of range 1000 from a [`FailingSource`], the entry, the blob, and the
/// offsets of the requests.
async fn failing_source(
    bytes: usize,
    status: Status,
) -> (Fetcher, ManifestEntry, Bytes, Arc<Mutex<Vec<u64>>>) {
    let http = Http::new().await;
    let entry = http.snapshot.manifest.entries()[1].clone();
    let blob = Bytes::from(http.blob(1_000));
    let offsets = Arc::default();
    let source = FailingSource { blob: blob.clone(), bytes, status, offsets: Arc::clone(&offsets) };
    (Fetcher::with_resume_rule(Box::new(source), ResumeRule::TEST), entry, blob, offsets)
}

#[tokio::test]
async fn a_transfer_that_fails_after_the_last_byte_gives_the_blob() {
    let (fetcher, entry, blob, offsets) =
        failing_source(usize::MAX, Status::aborted("cut after the last byte")).await;

    assert_eq!(fetcher.blob(&entry).await.unwrap(), blob);
    assert_eq!(*offsets.lock().unwrap(), [0]);
}

#[tokio::test]
async fn a_peer_refusal_stops_the_fetch_with_no_resume() {
    let (fetcher, entry, _, offsets) = failing_source(0, Status::not_found("no range")).await;

    let error = fetcher.blob(&entry).await.unwrap_err();

    assert!(matches!(&error, FetchError::Grpc(status) if status.code() == Code::NotFound));
    assert_eq!(*offsets.lock().unwrap(), [0]);
}

#[tokio::test]
async fn transfers_with_no_progress_stop_after_the_resume_limit() {
    let (fetcher, entry, _, offsets) = failing_source(0, Status::unavailable("down")).await;

    let error = fetcher.blob(&entry).await.unwrap_err();

    assert!(matches!(&error, FetchError::Grpc(status) if status.code() == Code::Unavailable));
    let requests = MAX_RESUMES_WITHOUT_PROGRESS as usize + 1;
    assert_eq!(*offsets.lock().unwrap(), vec![0; requests]);
}

#[tokio::test]
async fn a_stream_that_sends_no_byte_stops_the_fetch_with_no_resume() {
    let entry = SourceIndex::new(2).await.entries()[1].clone();
    let offsets = Arc::default();
    let source = SilentSource { offsets: Arc::clone(&offsets) };
    let fetcher = Fetcher::with_resume_rule(Box::new(source), ResumeRule::TEST);
    tokio::time::pause();
    let start = Instant::now();
    // The timer fires on a tick of 1 ms after the deadline.
    let at_the_limit = MAX_WAIT_WITHOUT_BYTES..=MAX_WAIT_WITHOUT_BYTES + Duration::from_millis(1);

    let error = fetcher.blob(&entry).await.unwrap_err();

    assert!(
        matches!(
            error,
            FetchError::Stalled { start_height: 1_000, offset: 0, waited }
                if at_the_limit.contains(&waited)
        ),
        "{error:?}"
    );
    assert!(at_the_limit.contains(&start.elapsed()), "{:?}", start.elapsed());
    assert_eq!(*offsets.lock().unwrap(), [0]);
    assert!(!error.is_transfer());
    assert!(error.to_string().contains("received no byte for 300 s"), "{error}");
    assert!(error.to_string().contains("use another --replicate-from source"), "{error}");
}

#[test]
fn an_error_chain_shows_each_source() {
    let error = FetchError::Digest { start_height: 7, mismatch: BlobMismatch::Digest };

    let chain = ErrorChain(&error).to_string();

    assert_eq!(chain, format!("{error}: {}", BlobMismatch::Digest));
}

#[tokio::test]
async fn an_http_source_sends_at_most_its_request_limit() {
    let mut http = Http::with_ranges(HTTP_BLOB_REQUESTS as u32 + 8).await;
    http.server = FileServer::start_with_answer_wait(http.snapshot.dir.path()).await;
    let fetcher = http.fetcher(SEED);
    let entries = http.snapshot.manifest.entries();

    let blobs = future::try_join_all(entries.iter().map(|entry| fetcher.blob(entry)));

    for (entry, blob) in entries.iter().zip(blobs.await.unwrap()) {
        assert_eq!(blob, http.blob(entry.start_height));
    }
    let most = http.server.most_open_requests();
    assert!((2..=HTTP_BLOB_REQUESTS).contains(&most), "{most} requests at one time");
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

    assert!(
        matches!(error, FetchError::Digest { start_height: 0, mismatch: BlobMismatch::Digest }),
        "{error:?}"
    );
}

#[tokio::test]
async fn peer_manifest_closes_the_last_epoch_at_the_last_range_and_blobs_resume() {
    let source = SourceIndex::new(RANGES).await;
    let (_sender, snapshots) = watch::channel(ServingSnapshot::from(source.state));
    let service = ReplicatorService::new(Arc::clone(&source.index), snapshots, &Network::Mainnet);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap()).parse().unwrap();
    let server = Server::builder().add_service(ReplicatorServer::new(service));
    tokio::spawn(server.serve_with_incoming(TcpListenerStream::new(listener)));
    let peer = PeerSource::connect(endpoint).await.unwrap();

    let manifest = peer.manifest().await.unwrap();

    // The rule closes no epoch at range 11. The manifest closes it there, as an export does.
    assert_eq!(manifest.entries(), source.entries());
    let entry = &manifest.entries()[2];
    let blob = sealed_range_blob(&source.index, 2_000);
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
        Self {
            source,
            server,
            fetcher: Fetcher::with_resume_rule(Box::new(peer), ResumeRule::TEST),
        }
    }

    /// Fetches the blobs of the first ten ranges at the same time, and checks each one.
    async fn fetch_ten_blobs_at_once(&self) {
        let entries = self.source.entries();
        let blobs = future::try_join_all(entries[..10].iter().map(|e| self.fetcher.blob(e)));
        for (entry, blob) in entries.iter().zip(blobs.await.unwrap()) {
            let expected = self
                .source
                .index
                .latest()
                .and_then(|snapshot| snapshot.sealed_range(entry.start_height));
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
    let waits: Vec<_> = RefusalBackoff::default().take(8).map(|wait| wait.as_millis()).collect();
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

    assert_eq!(blob, sealed_range_blob(&peer.source.index, 2_000));
    let half = blob.len() as u64 / 2;
    assert_eq!(peer.server.requests(), [(2_000, 0), (2_000, half)]);
    assert!(peer.server.refusals() > 0);
}

#[tokio::test]
async fn a_peer_that_refuses_every_stream_stops_the_fetch() {
    let source = SourceIndex::new(RANGES).await;
    let server = PeerServer::start(&source, 0).await;
    let peer = PeerSource::connect(server.endpoint.clone()).await.unwrap();
    let max_wait_without_bytes = Duration::from_millis(300);
    let rule = ResumeRule { max_wait_without_bytes, ..ResumeRule::TEST };
    let fetcher = Fetcher::with_resume_rule(Box::new(peer), rule);

    let error = fetcher.blob(&source.entries()[2]).await.unwrap_err();

    assert!(
        matches!(
            error,
            FetchError::Stalled { start_height: 2_000, offset: 0, waited }
                if waited >= max_wait_without_bytes
        ),
        "{error:?}"
    );
    assert!(server.refusals() > 1, "{} refusals", server.refusals());
    assert_eq!(server.requests(), []);
}

/// Twelve ranges: the filler closes an epoch only at range 9. The first export has the first
/// epoch. The second export, on the next day, has all ranges.
#[tokio::test]
async fn http_source_reads_the_manifest_of_each_export() {
    let source = SourceIndex::new(12).await;
    let dir = tempfile::tempdir().unwrap();
    let key = SigningKey::from_bytes(&SEED);
    let http = HttpSource::new(FileServer::start(dir.path()).await.base, key.verifying_key());
    let (http, writer) = (http.unwrap(), SnapshotWriter::new(dir.path(), key));
    let entries = source.entries();
    let manifest = |ranges| {
        let entries = entries[..ranges].to_vec();
        Manifest::new(Network::Mainnet.to_string(), Network::Mainnet.genesis_hash(), entries)
            .unwrap()
    };
    let now = DateTime::from_timestamp(1_792_972_800, 0).unwrap();

    for (ranges, written, now) in [(10, 10, now), (12, 2, now + TimeDelta::days(1))] {
        let exported = writer.export(&source.index, &manifest(ranges), now).unwrap();

        assert_eq!(exported.written, written);
        assert_eq!(http.manifest().await.unwrap(), manifest(ranges));
    }
}
