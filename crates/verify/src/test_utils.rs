//! Test fixtures: an index with `range_meta` rows, its snapshot directory, and an HTTP server
//! for that directory.

use std::{
    convert::Infallible,
    fs, io,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use ed25519_dalek::{Signer as _, SigningKey};
use http::{Request, Response, StatusCode, Uri, header::RANGE};
use http_body_util::{BodyExt as _, Full, StreamBody, combinators::BoxBody};
use hyper::{body::Frame, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use tempfile::TempDir;
use tokio::{
    net::TcpListener,
    sync::{mpsc, watch},
};
use tokio_stream::wrappers::ReceiverStream;
use zakura_chain::{block, parameters::Network};
use ztreamer_indexer::{
    codec::{CompactBlockRecord, TreeSizes},
    index::{
        Index, IndexState, RANGE_SIZE,
        range_meta::{BlobDigest, ManifestEntry},
    },
    parser::parse_block,
    test_utils::{populate, raw_block},
};
use ztreamer_service::{InProcessBackend, RangeMetaFiller, ServingSnapshot};

use crate::{
    manifest::Manifest,
    snapshot::{Latest, RangePath, SIGNATURE_SUFFIX},
};

/// The seed of the key that signs a [`Snapshot`].
pub(crate) const SEED: [u8; 32] = [7; 32];

/// An index with `ranges` sealed ranges of empty blocks, ten mutable blocks, and a `range_meta`
/// row for each sealed range from [`RangeMetaFiller`].
pub(crate) struct SourceIndex {
    pub(crate) index: Arc<Index>,
    pub(crate) state: IndexState,
    _dir: TempDir,
}

impl SourceIndex {
    pub(crate) async fn new(ranges: u32) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let index = Arc::new(Self::open(&dir));
        let records = Self::records(ranges * RANGE_SIZE + 10);
        let state = populate(&index, &records, Some(ranges * RANGE_SIZE - 1));
        let (_state, read, _tip, _change) = zakura_state::init(
            zakura_state::Config::ephemeral(),
            &Network::Mainnet,
            block::Height::MAX,
            0,
        )
        .await
        .unwrap();
        let (sender, snapshots) = watch::channel(ServingSnapshot::from(state));
        drop(sender);
        let backend = Arc::new(InProcessBackend::new(read));
        RangeMetaFiller::embedded(Arc::clone(&index), backend).run(snapshots).await;
        Self { index, state, _dir: dir }
    }

    /// An empty Mainnet index in `dir`.
    pub(crate) fn open(dir: &TempDir) -> Index {
        let network = Network::Mainnet;
        Index::open(dir.path(), 1 << 30, &network.to_string(), network.genesis_hash()).unwrap()
    }

    /// The records of [`raw_block`] from genesis, as the parser gives them.
    pub(crate) fn records(count: u32) -> Vec<CompactBlockRecord> {
        let mut sizes = TreeSizes::default();
        (0..count)
            .map(|height| {
                let record = parse_block(&raw_block(height)).unwrap().into_record(sizes).unwrap();
                sizes = record.end_tree_sizes;
                record
            })
            .collect()
    }

    /// The entries of the rows from genesis to the sealed tip, with `epoch_end` on the last
    /// entry, as `ztreamerd export-snapshot` writes them.
    pub(crate) fn entries(&self) -> Vec<ManifestEntry> {
        let ranges = self.state.sealed_through().unwrap() / RANGE_SIZE + 1;
        let mut entries: Vec<_> = (0..ranges)
            .map(|range| self.index.range_meta(range * RANGE_SIZE).unwrap().unwrap().entry)
            .collect();
        entries.last_mut().unwrap().epoch_end = true;
        entries
    }
}

/// A snapshot directory in the layout of `ztreamerd export-snapshot`, signed with [`SEED`].
pub(crate) struct Snapshot {
    pub(crate) manifest: Manifest,
    pub(crate) dir: TempDir,
}

impl Snapshot {
    /// The path of the manifest.
    pub(crate) const MANIFEST: &str = "manifests/2026-10-26T000000Z.json";

    pub(crate) fn export(source: &SourceIndex) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let manifest = Manifest::builder("Mainnet", Network::Mainnet.genesis_hash())
            .entries(source.entries())
            .build()
            .unwrap();
        for sub in ["ranges", "manifests"] {
            fs::create_dir(dir.path().join(sub)).unwrap();
        }
        let snapshot = Self { manifest, dir };
        for entry in snapshot.manifest.entries() {
            let blob = source.index.read_sealed_range_bytes(entry.start_height).unwrap().unwrap();
            fs::write(snapshot.range_path(entry), blob).unwrap();
        }
        snapshot.write_signed(Self::MANIFEST, &serde_json::to_vec(&snapshot.manifest).unwrap());
        let latest = Latest {
            manifest: Self::MANIFEST.to_owned(),
            sealed_through: snapshot.manifest.sealed_through(),
            published_at: "2026-10-26T00:00:00Z".to_owned(),
            // A source must not trust this key. Make it another key, so a test fails if it does.
            public_key: Some(SigningKey::from_bytes(&[8; 32]).verifying_key().to_bytes()),
        };
        let latest = latest.to_file(Some(&SigningKey::from_bytes(&SEED)));
        fs::write(snapshot.dir.path().join(Latest::PATH), latest).unwrap();
        snapshot
    }

    /// Replaces the blob of the range at `start` and its entry, and signs the new manifest, as a
    /// source that sends a wrong range does.
    pub(crate) fn replace_blob(&mut self, start: u32, blob: &[u8]) {
        let mut entries = self.manifest.entries().to_vec();
        let entry = &mut entries[(start / RANGE_SIZE) as usize];
        entry.blob_digest = BlobDigest(*blake3::hash(blob).as_bytes());
        entry.blob_len = blob.len() as u64;
        fs::write(self.range_path(entry), blob).unwrap();
        self.manifest = Manifest::builder("Mainnet", Network::Mainnet.genesis_hash())
            .entries(entries)
            .build()
            .unwrap();
        self.write_signed(Self::MANIFEST, &serde_json::to_vec(&self.manifest).unwrap());
    }

    /// The file of the blob of `entry`.
    pub(crate) fn range_path(&self, entry: &ManifestEntry) -> PathBuf {
        self.dir.path().join(RangePath::from(entry).as_str())
    }

    /// Writes `bytes` to `relative`, and their signature with `key` to its signature file.
    pub(crate) fn write_signed_with(&self, relative: &str, bytes: &[u8], key: &SigningKey) {
        let path = self.dir.path().join(relative);
        fs::write(&path, bytes).unwrap();
        let signature = format!("{}{SIGNATURE_SUFFIX}", path.display());
        fs::write(signature, hex::encode(key.sign(bytes).to_bytes())).unwrap();
    }

    fn write_signed(&self, relative: &str, bytes: &[u8]) {
        self.write_signed_with(relative, bytes, &SigningKey::from_bytes(&SEED));
    }
}

/// The requests that a [`FileServer`] got: the path and the `Range` header.
type Requests = Vec<(String, Option<String>)>;

/// An HTTP/1 server for the files below a directory. It answers a `Range: bytes=<offset>-`
/// request with 206 and the bytes from the offset.
pub(crate) struct FileServer {
    pub(crate) base: Uri,
    state: Arc<Served>,
}

struct Served {
    root: PathBuf,
    /// The next answer for this path sends half of its bytes, then stops with an error.
    cut: Mutex<Option<String>>,
    requests: Mutex<Requests>,
}

type Body = BoxBody<Bytes, io::Error>;

impl FileServer {
    pub(crate) async fn start(root: &Path) -> Self {
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
    pub(crate) fn cut_next(&self, path: &str) {
        *self.state.cut.lock().unwrap() = Some(path.to_owned());
    }

    pub(crate) fn requests(&self) -> Requests {
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
