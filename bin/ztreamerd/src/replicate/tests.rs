use std::{
    convert::Infallible,
    future,
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    sync::Mutex,
};

use bytes::Bytes;
use clap::Parser;
use ed25519_dalek::{Signer as _, SigningKey};
use http::{Request, Response, StatusCode};
use http_body_util::Full;
use hyper::{body::Incoming, server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::{
    task::{JoinHandle, JoinSet},
    time::timeout,
};
use tonic::transport::Server;
use tower::{Service as _, ServiceExt as _};
use zakura_chain::{
    block::{self, Block},
    serialization::ZcashSerialize as _,
};
use ztreamer_indexer::{
    codec::TreeSizes,
    index::{RANGE_SIZE, SEAL_DEPTH, range_meta::VerifyState},
    parser::parse_block,
    test_utils::{grow_chain, populate},
    types::RawIndexBlock,
};
use ztreamer_protocol::proto::{Empty, compact_tx_streamer_client::CompactTxStreamerClient};
use ztreamer_remote::test_utils::{MockChain, MockNode, Reply, Seen};
use ztreamer_service::{InProcessBackend, RangeMetaFiller, ServingSnapshot};
use ztreamer_verify::snapshot::{Latest, SIGNATURE_SUFFIX};

use super::*;
use crate::{Cli, Command, DaemonArgs};

/// The ranges of the snapshot: three epochs, the last with one range.
const RANGES: u32 = 21;

/// The sealed tip of the snapshot.
const SEALED: u32 = RANGES * RANGE_SIZE - 1;

/// The tip of the mock chain: the tail adds 20 blocks, and no range is sealed after the import.
const TIP: u32 = SEALED + 20 + SEAL_DEPTH;

/// The seed of the key that signs the snapshot.
const SEED: [u8; 32] = [7; 32];

/// A signed snapshot of the first [`RANGES`] ranges of a chain, its files on a local HTTP
/// server, and a file with its public key.
struct Snapshot {
    blocks: Vec<Block>,
    dir: TempDir,
    pubkey: PathBuf,
    server: FileServer,
    _files: TempDir,
}

impl Snapshot {
    /// Fills an index with the blocks of a chain to [`TIP`], seals it through [`SEALED`], fills
    /// its `range_meta` rows, and exports it with `ztreamerd export-snapshot`.
    async fn new() -> Self {
        let mut blocks = Vec::new();
        grow_chain(&mut blocks, TIP, 0);
        let source = tempfile::tempdir().unwrap();
        Self::index(&source, &blocks[..=usize::try_from(SEALED + 10).unwrap()]).await;

        let files = tempfile::tempdir().unwrap();
        let key = files.path().join("key");
        fs::write(&key, SEED).unwrap();
        let pubkey = files.path().join("key.pub");
        fs::write(&pubkey, hex::encode(SigningKey::from_bytes(&SEED).verifying_key().as_bytes()))
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = |dir: &TempDir| dir.path().to_str().unwrap().to_owned();
        let flags = ["export-snapshot", "--index-map-size", "1073741824", "--signing-key"];
        let flags = flags.into_iter().map(str::to_owned).chain([key.to_str().unwrap().to_owned()]);
        let flags = flags.chain(["--index-dir".into(), path(&source), "--out".into(), path(&dir)]);
        let Some(Command::ExportSnapshot(export)) =
            Cli::parse_from(["ztreamerd".to_owned()].into_iter().chain(flags)).command
        else {
            panic!("export-snapshot parses");
        };
        tokio::task::spawn_blocking(move || export.run()).await.unwrap().unwrap();
        let server = FileServer::start(dir.path()).await;
        Self { blocks, dir, pubkey, server, _files: files }
    }

    /// Writes `blocks` to an index in `dir`, sealed through [`SEALED`], with a `range_meta` row
    /// for each sealed range. Closes the index.
    async fn index(dir: &TempDir, blocks: &[Block]) {
        let network = Network::Mainnet;
        let index = Arc::new(
            Index::open(dir.path(), 1 << 30, &network.to_string(), network.genesis_hash()).unwrap(),
        );
        let mut sizes = TreeSizes::default();
        let records: Vec<_> = blocks
            .iter()
            .map(|block| {
                let raw = RawIndexBlock::from_bytes(block.zcash_serialize_to_vec().unwrap());
                let record = parse_block(&raw.unwrap()).unwrap().into_record(sizes).unwrap();
                sizes = record.end_tree_sizes;
                record
            })
            .collect();
        let state = populate(&index, &records, Some(SEALED));
        let (_state, read, _tip, _change) =
            zakura_state::init(zakura_state::Config::ephemeral(), &network, block::Height::MAX, 0)
                .await
                .unwrap();
        let (sender, snapshots) = watch::channel(ServingSnapshot::from(state));
        drop(sender);
        let backend = Arc::new(InProcessBackend::new(read));
        RangeMetaFiller::embedded(Arc::clone(&index), backend).run(snapshots).await;
    }

    /// The base URL of the snapshot.
    fn url(&self) -> String {
        self.server.base.clone()
    }

    /// Writes the manifest again for `network`, and signs it again.
    fn rename_network(&self, network: &str) {
        let key = SigningKey::from_bytes(&SEED);
        let latest = fs::read(self.dir.path().join(Latest::PATH)).unwrap();
        let latest = Latest::from_file(&latest, &key.verifying_key()).unwrap();
        let path = self.dir.path().join(latest.manifest);
        let mut manifest: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        manifest["network"] = json!(network);
        let manifest = serde_json::to_vec(&manifest).unwrap();
        fs::write(&path, &manifest).unwrap();
        let signature = format!("{}{SIGNATURE_SUFFIX}", path.display());
        fs::write(signature, hex::encode(key.sign(&manifest).to_bytes())).unwrap();
    }

    /// A mock node that serves the chain and the tree states of empty pools. It gives each
    /// tree state after `delay`.
    async fn node(&self, delay: Duration) -> (MockChain, MockNode) {
        let chain = MockChain::new(&self.blocks);
        let hashes: Vec<_> = self.blocks.iter().map(Block::hash).collect();
        let serve = chain.clone();
        let node = MockNode::start(move |_, request| match request.body["method"].as_str() {
            Some("z_gettreestate") => {
                // The script is synchronous. The test runtime has threads to spare.
                std::thread::sleep(delay);
                Self::treestate(&hashes, request)
            }
            _ => serve.reply(request),
        })
        .await;
        (chain, node)
    }

    /// The `z_gettreestate` answer of a chain with no shielded outputs.
    fn treestate(hashes: &[block::Hash], request: &Seen) -> Reply {
        let height: u32 = request.body["params"][0].as_str().unwrap().parse().unwrap();
        Reply::Result(json!({
            "hash": hashes[usize::try_from(height).unwrap()].to_string(),
            "height": height,
            "time": 0,
            "sapling": { "commitments": {} },
            "orchard": { "commitments": {} },
            "ironwood": { "commitments": {} },
        }))
    }
}

/// An HTTP/1 server for the files below a directory. It keeps the paths that it got.
struct FileServer {
    base: String,
    paths: Arc<Mutex<Vec<String>>>,
}

impl FileServer {
    async fn start(root: &Path) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let paths = Arc::new(Mutex::new(Vec::new()));
        let (root, seen) = (root.to_owned(), Arc::clone(&paths));
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let (root, seen) = (root.clone(), Arc::clone(&seen));
                let service = service_fn(move |request: Request<Incoming>| {
                    let path = request.uri().path().trim_start_matches('/').to_owned();
                    seen.lock().unwrap().push(path.clone());
                    let file = root.join(path);
                    async move {
                        let (status, bytes) = match tokio::fs::read(file).await {
                            Ok(bytes) => (StatusCode::OK, bytes),
                            Err(_) => (StatusCode::NOT_FOUND, Vec::new()),
                        };
                        let mut response = Response::new(Full::new(Bytes::from(bytes)));
                        *response.status_mut() = status;
                        Ok::<_, Infallible>(response)
                    }
                });
                tokio::spawn(http1::Builder::new().serve_connection(TokioIo::new(stream), service));
            }
        });
        Self { base, paths }
    }

    /// True if a request asked for a range file.
    fn fetched_a_range(&self) -> bool {
        self.paths.lock().unwrap().iter().any(|path| path.starts_with("ranges/"))
    }
}

/// A local address with no listener.
fn free_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap()
}

/// The flags of a replica of `snapshot` on the node at `url`, with the index in `dir`, then
/// `extra`.
fn cli(dir: &TempDir, url: &str, snapshot: &Snapshot, extra: &[&str]) -> DaemonArgs {
    let flags = [
        "--index-dir",
        dir.path().to_str().unwrap(),
        "--index-map-size",
        "1073741824",
        "--remote-rpc",
        url,
        "--replicate-from",
        &snapshot.url(),
        "--snapshot-pubkey",
        snapshot.pubkey.to_str().unwrap(),
    ];
    let flags = flags.into_iter().chain(extra.iter().copied());
    Cli::parse_from(["ztreamerd"].into_iter().chain(flags)).daemon
}

/// The daemon of `cli`, until `signal`.
fn daemon(
    cli: DaemonArgs,
    signal: impl Future<Output = Result<()>>,
) -> impl Future<Output = Result<()>> {
    let replica = cli.replicate.replica().unwrap();
    let node = cli.remote.node().unwrap().unwrap().with_replica(replica);
    node.run_until(cli, Server::builder(), signal)
}

/// Runs the replica with no signal, until it stops by itself, and gives the error.
async fn refusal(cli: DaemonArgs) -> eyre::Report {
    let run = timeout(Duration::from_secs(30), daemon(cli, future::pending()));
    run.await.expect("the replica stops by itself").unwrap_err()
}

/// Asserts that a cause in the chain of `error` is `expected`.
fn assert_cause(error: &eyre::Report, expected: &str) {
    assert!(error.chain().any(|cause| cause.to_string() == expected), "{error:?}");
}

/// A replica that serves gRPC on a free local port until [`Self::stop`] sends the signal.
struct RunningReplica {
    grpc: SocketAddr,
    signal: CancellationToken,
    task: JoinHandle<Result<()>>,
    dir: TempDir,
}

impl RunningReplica {
    /// Starts a replica of `snapshot` with a fresh index, on `node`, with `extra` flags.
    fn start(snapshot: &Snapshot, node: &MockNode, extra: &[&str]) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let grpc = free_addr();
        let grpc_flag = grpc.to_string();
        let flags: Vec<_> =
            ["--grpc-listen", &grpc_flag].into_iter().chain(extra.iter().copied()).collect();
        let cli = cli(&dir, &node.url().to_string(), snapshot, &flags);
        let signal = CancellationToken::new();
        let received = signal.clone().cancelled_owned();
        let task = tokio::spawn(daemon(cli, async move {
            received.await;
            Ok(())
        }));
        Self { grpc, signal, task, dir }
    }

    /// The readiness text of the first `GetLightdInfo` answer that contains `text`.
    async fn readiness_with(&self, text: &str) -> String {
        let poll = async {
            loop {
                if let Ok(mut client) =
                    CompactTxStreamerClient::connect(format!("http://{}", self.grpc)).await
                    && let Ok(info) = client.get_lightd_info(Empty {}).await
                    && info.get_ref().build_user.contains(text)
                {
                    return info.into_inner().build_user;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        };
        timeout(Duration::from_secs(60), poll).await.expect("the replica reports the text")
    }

    /// Sends the signal. The replica must stop without an error. Returns its index.
    async fn stop(self) -> Index {
        self.signal.cancel();
        timeout(Duration::from_secs(10), self.task).await.unwrap().unwrap().unwrap();
        Index::open_existing(self.dir.path(), 1 << 30).unwrap()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fresh_replica_imports_three_epochs_then_follows_the_chain() {
    let snapshot = Snapshot::new().await;
    let (_chain, node) = snapshot.node(Duration::ZERO).await;

    let replica = RunningReplica::start(&snapshot, &node, &[]);
    let readiness = replica.readiness_with("import_state=following").await;
    let index = replica.stop().await;

    assert_eq!(
        readiness,
        format!(
            "sealed_through={SEALED} verified_through={SEALED} chain_ok_through=none \
             import_state=following node_state=ok import_sources={}/:0",
            snapshot.url()
        )
    );
    // The tail indexed the blocks above the snapshot. The head follower keeps the last ones.
    assert!(index.state().unwrap().durable_tip().unwrap().height > SEALED);
    assert_eq!(index.state().unwrap().verified_through(), Some(SEALED));
    assert_eq!(index.range_meta(12_000).unwrap().unwrap().state, VerifyState::Verified);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unverified_replica_serves_then_catches_up_without_a_restart() {
    let snapshot = Snapshot::new().await;
    // Three tree states, one per epoch: the proofs end about 1.5 s after the replica serves.
    let (_chain, node) = snapshot.node(Duration::from_millis(500)).await;

    let replica = RunningReplica::start(&snapshot, &node, &["--serve-unverified"]);
    let verifying = replica.readiness_with("import_state=verifying").await;
    let following = replica.readiness_with("import_state=following").await;
    let index = replica.stop().await;

    let chain_ok = format!("sealed_through={SEALED} verified_through=");
    assert!(verifying.starts_with(&chain_ok), "{verifying}");
    assert!(!verifying.starts_with(&format!("{chain_ok}{SEALED} ")), "{verifying}");
    assert!(verifying.contains(&format!("chain_ok_through={SEALED} ")), "{verifying}");
    let verified = format!("verified_through={SEALED} chain_ok_through={SEALED} ");
    assert!(following.contains(&verified), "{following}");
    assert_eq!(index.state().unwrap().verified_through(), Some(SEALED));
    assert_eq!(index.range_meta(20_000).unwrap().unwrap().state, VerifyState::Verified);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_of_another_chain_is_refused_before_a_range_fetch() {
    let snapshot = Snapshot::new().await;
    let chain = MockChain::new(&snapshot.blocks);
    let node = MockNode::start(move |_, request| match chain.reply(request) {
        Reply::Result(mut info) if request.body["method"] == "getblockchaininfo" => {
            info["chain"] = json!("test");
            Reply::Result(info)
        }
        reply => reply,
    })
    .await;
    let dir = tempfile::tempdir().unwrap();

    let error = refusal(cli(&dir, &node.url().to_string(), &snapshot, &[])).await;

    assert_cause(&error, r#"remote zakura runs chain "test", the index needs "main""#);
    assert!(!snapshot.server.fetched_a_range());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_snapshot_of_another_network_is_refused_before_a_range_fetch() {
    let snapshot = Snapshot::new().await;
    snapshot.rename_network("Testnet");
    let (_chain, node) = snapshot.node(Duration::ZERO).await;
    let dir = tempfile::tempdir().unwrap();

    let error = refusal(cli(&dir, &node.url().to_string(), &snapshot, &[])).await;

    assert_eq!(error.to_string(), "the snapshot is for network Testnet, the index for Mainnet");
    assert!(!snapshot.server.fetched_a_range());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_manifest_that_another_key_signs_is_refused_before_a_range_fetch() {
    let snapshot = Snapshot::new().await;
    let other = SigningKey::from_bytes(&[8; 32]).verifying_key();
    fs::write(&snapshot.pubkey, hex::encode(other.as_bytes())).unwrap();
    let (_chain, node) = snapshot.node(Duration::ZERO).await;
    let dir = tempfile::tempdir().unwrap();

    let error = refusal(cli(&dir, &node.url().to_string(), &snapshot, &[])).await;

    assert_eq!(error.to_string(), format!("read the manifest of {}/", snapshot.url()));
    assert!(error.chain().any(|cause| cause.to_string().contains("signature")), "{error:?}");
    assert!(!snapshot.server.fetched_a_range());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_that_prunes_the_attested_heights_is_refused_before_a_range_fetch() {
    let snapshot = Snapshot::new().await;
    let (chain, node) = snapshot.node(Duration::ZERO).await;
    chain.set_prune_height(Some(block::Height(15_000)));
    let dir = tempfile::tempdir().unwrap();

    let error = refusal(cli(&dir, &node.url().to_string(), &snapshot, &[])).await;

    assert_eq!(
        error.to_string(),
        "the import attests the tree states from height 9999 on, but the remote zakura node \
         keeps block bodies only from height 15000: use a node that does not prune, or an index \
         that is verified through height 14999 or above"
    );
    assert!(!snapshot.server.fetched_a_range());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unreachable_node_is_refused_before_a_range_fetch() {
    let snapshot = Snapshot::new().await;
    let dir = tempfile::tempdir().unwrap();
    let node = free_addr();

    let error = refusal(cli(&dir, &format!("http://{node}"), &snapshot, &[])).await;

    assert_eq!(error.to_string(), format!("the remote zakura node at {node} is unreachable"));
    assert!(!snapshot.server.fetched_a_range());
}

/// A replica of `snapshot` in `dir` on the embedded node: the flags without `--remote-rpc`.
async fn embedded_replica(dir: &TempDir, snapshot: &Snapshot) -> (Replica, Arc<Index>) {
    let flags = [
        "--index-dir",
        dir.path().to_str().unwrap(),
        "--index-map-size",
        "1073741824",
        "--replicate-from",
        &snapshot.url(),
        "--snapshot-pubkey",
        snapshot.pubkey.to_str().unwrap(),
    ];
    let cli = Cli::parse_from(["ztreamerd"].into_iter().chain(flags)).daemon;
    assert!(cli.remote.node().unwrap().is_none());
    let index = cli.open_index(&Network::Mainnet).await.unwrap();
    (cli.replicate.replica().unwrap().unwrap(), index)
}

/// An ephemeral mainnet zakura state with `blocks`, committed without the checks of the
/// verifiers. The first value writes the state: keep it while the state is in use.
async fn embedded_state(blocks: &[Block]) -> (impl Sized, ReadStateService) {
    let (mut state, read, _tip, _change) = zakura_state::init(
        zakura_state::Config::ephemeral(),
        &Network::Mainnet,
        block::Height::MAX,
        0,
    )
    .await
    .unwrap();
    // Send all commits before the first answer, as `zakura_state::populated_state` does. The
    // state commits them in order.
    let mut commits = JoinSet::new();
    for block in blocks {
        let block = Arc::new(block.clone());
        let request = zakura_state::Request::CommitCheckpointVerifiedBlock(block.into());
        commits.spawn(state.ready().await.unwrap().call(request));
    }
    while let Some(commit) = commits.join_next().await {
        commit.unwrap().unwrap();
    }
    (state, read)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_embedded_node_verifies_an_import_of_three_epochs() {
    let snapshot = Snapshot::new().await;
    let (_writer, state) = embedded_state(&snapshot.blocks[..=SEALED as usize]).await;
    let dir = tempfile::tempdir().unwrap();
    let (replica, index) = embedded_replica(&dir, &snapshot).await;
    let status = replica.new_status();

    let flow = replica
        .import(&state, &Network::Mainnet, &index, &status, &CancellationToken::new())
        .await
        .unwrap();

    assert!(matches!(flow, ControlFlow::Continue(None)), "{flow:?}");
    assert_eq!(index.state().unwrap().sealed_through(), Some(SEALED));
    assert_eq!(index.state().unwrap().verified_through(), Some(SEALED));
    let meta = index.range_meta(12_000).unwrap().unwrap();
    assert_eq!(meta.state, VerifyState::Verified);
    assert_eq!(meta.verified_by.unwrap().url, "embedded");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_embedded_node_without_the_blocks_gives_no_answer_and_installs_nothing() {
    let snapshot = Snapshot::new().await;
    let (_writer, state) = embedded_state(&snapshot.blocks[..1]).await;
    let dir = tempfile::tempdir().unwrap();
    let (replica, index) = embedded_replica(&dir, &snapshot).await;
    let status = replica.new_status();

    let error = replica
        .import(&state, &Network::Mainnet, &index, &status, &CancellationToken::new())
        .await
        .unwrap_err();

    assert_cause(&error, "node has no block at height 999");
    assert_eq!(index.state().unwrap().sealed_through(), None);
}

#[test]
fn the_embedded_mode_without_the_flag_has_no_import() {
    let cli = Cli::parse_from(["ztreamerd", "--zakura-config", "zakura.toml"]).daemon;

    assert!(cli.remote.node().unwrap().is_none());
    assert!(cli.replicate.replica().unwrap().is_none());
}

#[test]
fn the_flags_need_a_key_for_a_snapshot() {
    let parse = |flags: &[&str]| {
        Cli::try_parse_from(["ztreamerd"].iter().chain(flags)).map(|cli| cli.daemon)
    };
    let rpc = ["--remote-rpc", "http://127.0.0.1:8232"];
    assert!(parse(&["--snapshot-pubkey", "key.pub"]).is_err());
    let embedded = parse(&["--zakura-config", "zakura.toml", "--replicate-from", "peer:9068"]);
    assert!(embedded.unwrap().replicate.replica().unwrap().is_some());
    assert!(parse(&[rpc[0], rpc[1], "--replicate-from", "peer"]).is_err());
    assert!(parse(&[rpc[0], rpc[1], "--replicate-from", "peer:9068/path"]).is_err());

    let peer = parse(&[rpc[0], rpc[1], "--replicate-from", "peer:9068"]).unwrap();
    assert_eq!(
        peer.replicate.replicate_from,
        [ReplicaSource::Peer("http://peer:9068".parse().unwrap())]
    );
    assert_eq!(peer.replicate.replicate_from[0].to_string(), "peer:9068");
    assert_eq!(peer.replicate.verify_sample, 2);
    assert!(peer.replicate.replica().unwrap().is_some());
    assert!(parse(&[]).unwrap().replicate.replica().unwrap().is_none());

    let https = ["--replicate-from", "https://snapshots.example/mainnet"];
    let error =
        parse(&[rpc[0], rpc[1], https[0], https[1]]).unwrap().replicate.replica().unwrap_err();
    assert_eq!(
        error.to_string(),
        "--replicate-from https://snapshots.example/mainnet needs --snapshot-pubkey: ztreamerd \
         has no built-in key"
    );
    let two = [rpc[0], rpc[1], "--replicate-from", "peer:9068", "--replicate-from", "peer:9069"];
    let replica = parse(&two).unwrap().replicate.replica().unwrap().unwrap();
    let names: Vec<_> = replica
        .new_status()
        .progress()
        .import_sources
        .into_iter()
        .map(|source| source.name)
        .collect();
    assert_eq!(names, ["peer:9068", "peer:9069"]);
}
