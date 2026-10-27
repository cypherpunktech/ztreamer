use std::{fs, future, net::SocketAddr, path::Path, time::Duration};

use clap::Parser;
use ed25519_dalek::SigningKey;
use serde_json::json;
use tempfile::TempDir;
use tokio::time::timeout;
use tonic::transport::Server;
use zakura_chain::{block::Block, serialization::ZcashSerialize as _};
use ztreamer_indexer::{
    codec::CompactBlockRecord,
    index::{RANGE_SIZE, SEAL_DEPTH, range_meta::VerifyState},
    test_utils::grow_chain,
    types::RawIndexBlock,
};
use ztreamer_protocol::{
    proto::BlockId,
    replicate::{ListRangesRequest, replicator_client::ReplicatorClient},
};
use ztreamer_remote::test_utils::{MockChain, MockNode, Reply};
use ztreamer_verify::{
    fetch::{HttpSource, PeerSource},
    test_utils::{FileServer, SourceIndex},
};

use super::*;
use crate::{
    Cli, Command, DaemonArgs,
    remote::RemoteNode,
    test_support::{free_addr, latest_block_at, node_with_chain_name},
};

/// The ranges of the snapshot: three epochs, the last with one range.
const RANGES: u32 = 21;

/// The sealed tip of the snapshot.
const SEALED: u32 = RANGES * RANGE_SIZE - 1;

/// The tip of the mock chain: the tail adds 20 blocks, and no range is sealed after the import.
const TIP: u32 = SEALED + 20 + SEAL_DEPTH;

/// The seed of the key that signs the snapshot.
const SEED: [u8; 32] = [7; 32];

/// A snapshot of the first [`RANGES`] ranges of a chain, signed with [`SEED`], on a local HTTP
/// server.
struct Snapshot {
    blocks: Vec<Block>,
    server: FileServer,
    _dir: TempDir,
}

impl Snapshot {
    /// The snapshot of a chain with its tip at [`TIP`].
    async fn new() -> Self {
        Self::with_tip(TIP).await
    }

    /// Fills an index with the blocks of a chain with its tip at `tip`, seals it through
    /// [`SEALED`], fills its `range_meta` rows, and exports it with `ztreamerd export-snapshot`.
    async fn with_tip(tip: u32) -> Self {
        let mut blocks = Vec::new();
        grow_chain(&mut blocks, tip, 0);
        let records = Self::records(&blocks[..=usize::try_from(SEALED + 10).unwrap()]);
        let source = SourceIndex::with_range_meta(&records, SEALED).await.into_dir();
        let dir = tempfile::tempdir().unwrap();
        Self::export(source.path(), dir.path()).await;
        let server = FileServer::start(&dir.path().join("snapshot")).await;
        Self { blocks, server, _dir: dir }
    }

    /// Exports the index in `index` with `ztreamerd export-snapshot` to `dir/snapshot`, signed
    /// with [`SEED`].
    async fn export(index: &Path, dir: &Path) {
        let key = dir.join("key");
        fs::write(&key, SEED).unwrap();
        let out = dir.join("snapshot");
        let [key, index, out] = [key.as_path(), index, &out].map(|p| p.to_str().unwrap());
        let flags = ["--index-map-size", "1073741824", "--signing-key", key, "--index-dir", index];
        let flags = ["ztreamerd", "export-snapshot", "--out", out].into_iter().chain(flags);
        let Cli { command: Some(Command::ExportSnapshot(export)), daemon } = Cli::parse_from(flags)
        else {
            panic!("export-snapshot parses");
        };
        let export = move || export.run(&daemon.open_existing_index()?);
        tokio::task::spawn_blocking(export).await.unwrap().unwrap();
    }

    /// The height of the tip of the chain.
    fn tip(&self) -> u32 {
        u32::try_from(self.blocks.len() - 1).unwrap()
    }

    /// The records of `blocks` from genesis, as the parser gives them.
    fn records(blocks: &[Block]) -> Vec<CompactBlockRecord> {
        SourceIndex::parse_records(blocks.iter().map(|block| {
            RawIndexBlock::from_bytes(block.zcash_serialize_to_vec().unwrap()).unwrap()
        }))
    }

    /// A mock node that serves the chain, and the tree states of empty pools at each block. A
    /// tree state request names a height (the attestor of the import) or a hash (the filler).
    async fn node(&self) -> (MockChain, MockNode) {
        let chain = MockChain::new(&self.blocks);
        let hashes: Vec<_> = self.blocks.iter().map(Block::hash).collect();
        let serve = chain.clone();
        let node = MockNode::start(move |_, request| {
            if request.body["method"] != "z_gettreestate" {
                return serve.reply(request);
            }
            let block = request.body["params"][0].as_str().unwrap();
            let height = block.parse().ok().unwrap_or_else(|| {
                let at = hashes.iter().position(|hash| hash.to_string() == block).unwrap();
                u32::try_from(at).unwrap()
            });
            let empty = json!({ "commitments": {} });
            Reply::Result(json!({
                "hash": hashes[usize::try_from(height).unwrap()].to_string(),
                "height": height,
                "time": 0,
                "sapling": empty, "orchard": empty, "ironwood": empty,
            }))
        })
        .await;
        (chain, node)
    }

    /// True if a request to the snapshot server asked for a range file.
    fn fetched_a_range(&self) -> bool {
        self.server.requests().iter().any(|(path, _)| path.starts_with("ranges/"))
    }
}

/// The flags of a replica of `snapshot` on the node at `url`, with the index in `dir`, then
/// `extra`.
fn cli(dir: &TempDir, url: &str, snapshot: &Snapshot, extra: &[&str]) -> DaemonArgs {
    let dir = dir.path().to_str().unwrap();
    let flags = ["--index-dir", dir, "--index-map-size", "1073741824", "--remote-rpc", url];
    let base = snapshot.server.base.to_string();
    let flags = flags.into_iter().chain(["--replicate-from", &base]);
    Cli::parse_from(["ztreamerd"].into_iter().chain(flags).chain(extra.iter().copied())).daemon
}

/// The daemon of `cli` until `signal`, with the snapshot key of `seed` in place of the built-in
/// key, and with no default cookie file.
async fn daemon(
    cli: DaemonArgs,
    seed: [u8; 32],
    signal: impl Future<Output = Result<()>>,
) -> Result<()> {
    let replica =
        Replica { key: SigningKey::from_bytes(&seed).verifying_key(), ..cli.replicate.replica() };
    let client = cli.remote.client(Path::new("/nonexistent/zakura/.cookie")).unwrap().unwrap();
    RemoteNode::new(client, replica).run_until(cli, Server::builder(), signal).await
}

/// Runs the replica with no signal until it stops by itself, and gives the error. Asserts that
/// no range was fetched.
async fn refusal(snapshot: &Snapshot, url: &str, seed: [u8; 32]) -> eyre::Report {
    let dir = tempfile::tempdir().unwrap();
    let run = timeout(
        Duration::from_secs(30),
        daemon(cli(&dir, url, snapshot, &[]), seed, future::pending()),
    );
    let error = run.await.expect("the replica stops by itself").unwrap_err();
    assert!(!snapshot.fetched_a_range(), "{error:?}");
    error
}

/// Runs a replica of `snapshot` on `node` with the index in `dir`, until it serves the tip of the
/// chain. Then stops it, and gives the tip that it served.
async fn serve_until_tip(dir: &TempDir, snapshot: &Snapshot, node: &MockNode) -> BlockId {
    serve_until(dir, snapshot, node, async |_| ()).await.0
}

/// Runs a replica of `snapshot` on `node` with the index in `dir`, until it serves the tip of the
/// chain and `served` returns. `served` gets the gRPC address of the replica. Then stops the
/// replica, and gives the tip that it served and the result of `served`.
async fn serve_until<T>(
    dir: &TempDir,
    snapshot: &Snapshot,
    node: &MockNode,
    served: impl AsyncFnOnce(SocketAddr) -> T,
) -> (BlockId, T) {
    let grpc = free_addr();
    let cli = cli(dir, &node.url().to_string(), snapshot, &["--grpc-listen", &grpc.to_string()]);
    let signal = CancellationToken::new();
    let received = signal.clone().cancelled_owned();
    let task = tokio::spawn(daemon(cli, SEED, async move {
        received.await;
        Ok(())
    }));

    // The servers start after the import and the tail. The tip is ready after the first head
    // reconcile.
    let latest = latest_block_at(grpc, snapshot.tip(), Duration::from_secs(60)).await;
    let result = served(grpc).await;
    signal.cancel();
    timeout(Duration::from_secs(10), task).await.unwrap().unwrap().unwrap();
    (latest, result)
}

/// The start heights of the ranges that the Replicator at `grpc` lists, when it lists `count`
/// ranges. Polls every 50 ms, and panics after 30 s.
async fn listed_ranges(grpc: SocketAddr, count: usize) -> Vec<u32> {
    let poll = async {
        loop {
            let mut client = ReplicatorClient::connect(format!("http://{grpc}")).await.unwrap();
            let request = ListRangesRequest { from_start_height: 0 };
            let mut listed = client.list_ranges(request).await.unwrap().into_inner();
            let mut starts = Vec::new();
            while let Some(entry) = listed.message().await.unwrap() {
                starts.push(entry.start_height);
            }
            if starts.len() == count {
                return starts;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(Duration::from_secs(30), poll).await.expect("the Replicator lists the ranges")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fresh_replica_imports_three_epochs_then_follows_the_chain() {
    let snapshot = Snapshot::new().await;
    let (_chain, node) = snapshot.node().await;
    let dir = tempfile::tempdir().unwrap();

    let latest = serve_until_tip(&dir, &snapshot, &node).await;

    assert_eq!(latest.hash, snapshot.blocks.last().unwrap().hash().0);
    let index = Index::open_existing(dir.path(), 1 << 30).unwrap();
    assert_eq!(index.state().unwrap().verified_through(), Some(SEALED));
    assert_eq!(index.range_meta(12_000).unwrap().unwrap().state, VerifyState::Verified);
}

/// The tail seals two ranges after the import. The filler writes their rows from the node of
/// `--remote-rpc`. Then the Replicator lists all ranges, a peer gives the ranges through the last
/// epoch end, and `export-snapshot` of the replica gives a manifest that passes the epoch rule.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replica_fills_the_rows_of_the_ranges_that_the_tail_seals() {
    const TAIL_SEALED: u32 = SEALED + 2 * RANGE_SIZE;
    let snapshot = Snapshot::with_tip(TAIL_SEALED + 20 + SEAL_DEPTH).await;
    let (_chain, node) = snapshot.node().await;
    let dir = tempfile::tempdir().unwrap();

    let (_latest, (listed, peer)) = serve_until(&dir, &snapshot, &node, async |grpc| {
        let listed = listed_ranges(grpc, RANGES as usize + 2).await;
        let peer = PeerSource::connect(format!("http://{grpc}").parse().unwrap()).await.unwrap();
        (listed, peer.manifest().await.unwrap())
    })
    .await;

    assert_eq!(listed.last(), Some(&(TAIL_SEALED + 1 - RANGE_SIZE)));
    // The last imported range, 20,000, ends a short epoch of the snapshot only. The two tail
    // ranges continue that epoch, thus the last epoch end of the peer is 19,999.
    assert_eq!(peer.sealed_through(), 19_999);
    {
        let index = Index::open_existing(dir.path(), 1 << 30).unwrap();
        let imported = index.range_meta(20_000).unwrap().unwrap();
        let filled = index.range_meta(TAIL_SEALED + 1 - RANGE_SIZE).unwrap().unwrap();
        assert!(!imported.entry.epoch_end);
        assert_eq!(filled.state, VerifyState::Verified);
        assert_eq!(filled.verified_by, imported.verified_by);
        // The tail ranges join the verified prefix, as the rows say.
        assert_eq!(index.state().unwrap().verified_through(), Some(TAIL_SEALED));
    }
    let out = tempfile::tempdir().unwrap();
    Snapshot::export(dir.path(), out.path()).await;
    let server = FileServer::start(&out.path().join("snapshot")).await;
    let key = SigningKey::from_bytes(&SEED).verifying_key();
    let exported = HttpSource::new(server.base.clone(), key).unwrap().manifest().await.unwrap();
    assert_eq!(exported.sealed_through(), TAIL_SEALED);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_index_only_replica_stops_after_the_import_and_the_tail() {
    let snapshot = Snapshot::new().await;
    let (_chain, node) = snapshot.node().await;
    let dir = tempfile::tempdir().unwrap();
    let cli = cli(&dir, &node.url().to_string(), &snapshot, &["--index-only"]);

    let run = timeout(Duration::from_secs(60), daemon(cli, SEED, future::pending()));
    run.await.expect("the replica stops by itself").unwrap();

    let state = Index::open_existing(dir.path(), 1 << 30).unwrap().state().unwrap();
    assert_eq!(state.verified_through(), Some(SEALED));
    assert_eq!(state.durable_tip().map(|tip| tip.height), Some(TIP - SEAL_DEPTH));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replica_that_the_tail_sealed_continues_with_the_tail() {
    let snapshot = Snapshot::new().await;
    let (_chain, node) = snapshot.node().await;
    // As after the tail of an older manifest, with no `range_meta` row: one range past the epoch
    // end 9,999 of the snapshot (`EpochBoundary`), then exactly at it (`MissingRow`).
    for sealed in [10_999, 9_999] {
        let blocks = &snapshot.blocks[..=usize::try_from(sealed + 10).unwrap()];
        let dir = SourceIndex::sealed(&Snapshot::records(blocks), sealed).into_dir();

        let latest = serve_until_tip(&dir, &snapshot, &node).await;

        assert_eq!(latest.hash, snapshot.blocks.last().unwrap().hash().0);
        assert!(!snapshot.fetched_a_range());
        let index = Index::open_existing(dir.path(), 1 << 30).unwrap();
        assert_eq!(index.state().unwrap().verified_through(), None);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_refusal_stops_the_replica_before_a_range_fetch() {
    let snapshot = Snapshot::new().await;
    let (chain, node) = snapshot.node().await;
    let url = node.url().to_string();

    let error = refusal(&snapshot, &url, [8; 32]).await;
    assert_eq!(error.to_string(), "import the sealed ranges");
    assert!(error.chain().any(|cause| cause.to_string().contains("signature")), "{error:?}");

    // A node of the test chain gives the testnet index, and its genesis block is not the one
    // of testnet.
    let other = node_with_chain_name("test", &chain).await;
    let error = refusal(&snapshot, &other.url().to_string(), SEED).await;
    let genesis = |cause: &(dyn std::error::Error + 'static)| {
        cause.to_string().starts_with("remote zakura has genesis block")
    };
    assert!(error.chain().any(genesis), "{error:?}");
}

#[test]
fn replicate_from_takes_one_snapshot_or_one_peer() {
    let parse = |flags: &[&str]| {
        let remote = ["ztreamerd", "--remote-rpc", "http://127.0.0.1:8232"];
        Cli::try_parse_from(remote.iter().chain(flags)).map(|cli| cli.daemon)
    };
    assert!(parse(&["--replicate-from", "peer"]).is_err());
    assert!(parse(&["--replicate-from", "peer:9068/path"]).is_err());
    let two =
        ["--replicate-from", "https://snapshots.example/mainnet", "--replicate-from", "peer:9068"];
    assert!(parse(&two).is_err());
    let snapshot = parse(&two[..2]).unwrap().replicate.replica();
    assert_eq!(snapshot.source.to_string(), "https://snapshots.example/mainnet");
    let peer = parse(&two[2..]).unwrap().replicate.replica();
    assert_eq!(peer.source.to_string(), "peer:9068");
    assert_eq!(peer.source, ReplicaSource::Peer("http://peer:9068".parse().unwrap()));
}
