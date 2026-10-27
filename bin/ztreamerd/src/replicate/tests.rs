use std::{fs, future, time::Duration};

use clap::Parser;
use ed25519_dalek::SigningKey;
use serde_json::json;
use tempfile::TempDir;
use tokio::time::timeout;
use tonic::transport::Server;
use zakura_chain::{
    block::{self, Block},
    serialization::ZcashSerialize as _,
};
use ztreamer_indexer::{
    codec::CompactBlockRecord,
    index::{RANGE_SIZE, SEAL_DEPTH, range_meta::VerifyState},
    test_utils::grow_chain,
    types::RawIndexBlock,
};
use ztreamer_protocol::proto::BlockId;
use ztreamer_remote::test_utils::{MockChain, MockNode, Reply};
use ztreamer_verify::test_utils::{FileServer, SourceIndex};

use super::*;
use crate::{
    Cli, Command, DaemonArgs, run_daemon,
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
    /// Fills an index with the blocks of a chain, seals it through [`SEALED`], fills its
    /// `range_meta` rows, and exports it with `ztreamerd export-snapshot`.
    async fn new() -> Self {
        let mut blocks = Vec::new();
        grow_chain(&mut blocks, TIP, 0);
        let records = Self::records(&blocks[..=usize::try_from(SEALED + 10).unwrap()]);
        let source = SourceIndex::with_range_meta(&records, SEALED).await.into_dir();
        let dir = tempfile::tempdir().unwrap();
        let key = dir.path().join("key");
        fs::write(&key, SEED).unwrap();
        let out = dir.path().join("snapshot");
        let [key, source, out] = [key.as_path(), source.path(), &out].map(|p| p.to_str().unwrap());
        let flags = ["--index-map-size", "1073741824", "--signing-key", key, "--index-dir", source];
        let flags = ["ztreamerd", "export-snapshot", "--out", out].into_iter().chain(flags);
        let Cli { command: Some(Command::ExportSnapshot(export)), daemon } = Cli::parse_from(flags)
        else {
            panic!("export-snapshot parses");
        };
        let export = move || export.run(&daemon.open_existing_index()?);
        tokio::task::spawn_blocking(export).await.unwrap().unwrap();
        let server = FileServer::start(&dir.path().join("snapshot")).await;
        Self { blocks, server, _dir: dir }
    }

    /// The records of `blocks` from genesis, as the parser gives them.
    fn records(blocks: &[Block]) -> Vec<CompactBlockRecord> {
        SourceIndex::parse_records(blocks.iter().map(|block| {
            RawIndexBlock::from_bytes(block.zcash_serialize_to_vec().unwrap()).unwrap()
        }))
    }

    /// A mock node that serves the chain, and the tree states of empty pools at each height.
    async fn node(&self) -> (MockChain, MockNode) {
        let chain = MockChain::new(&self.blocks);
        let hashes: Vec<_> = self.blocks.iter().map(Block::hash).collect();
        let serve = chain.clone();
        let node = MockNode::start(move |_, request| {
            if request.body["method"] != "z_gettreestate" {
                return serve.reply(request);
            }
            let height: u32 = request.body["params"][0].as_str().unwrap().parse().unwrap();
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
/// key.
async fn daemon(
    cli: DaemonArgs,
    seed: [u8; 32],
    signal: impl Future<Output = Result<()>>,
) -> Result<()> {
    let mut replica = cli.replicate.replica().unwrap();
    replica.key = SigningKey::from_bytes(&seed).verifying_key();
    let node = cli.remote.node().unwrap().unwrap().with_replica(Some(replica));
    node.run_until(cli, Server::builder(), signal).await
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
    let latest = latest_block_at(grpc, TIP, Duration::from_secs(60)).await;
    signal.cancel();
    timeout(Duration::from_secs(10), task).await.unwrap().unwrap().unwrap();
    latest
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

    chain.set_prune_height(Some(block::Height(15_000)));
    let error = refusal(&snapshot, &url, SEED).await;
    assert_eq!(
        error.to_string(),
        "the import needs block 0, but the remote zakura node keeps block bodies only from height \
         15000: use a node that does not prune, or an index that is sealed through height \
         14999 or above"
    );

    // A node of the test chain gives the testnet index, and its genesis block is not the one
    // of testnet. The genesis check comes before the prune check.
    let other = node_with_chain_name("test", &chain).await;
    let error = refusal(&snapshot, &other.url().to_string(), SEED).await;
    let genesis = |cause: &(dyn std::error::Error + 'static)| {
        cause.to_string().starts_with("remote zakura has genesis block")
    };
    assert!(error.chain().any(genesis), "{error:?}");
}

#[test]
fn replicate_from_takes_snapshots_and_peers_and_needs_the_remote_mode() {
    let parse = |flags: &[&str]| {
        Cli::try_parse_from(["ztreamerd"].iter().chain(flags)).map(|cli| cli.daemon)
    };
    assert!(parse(&["--replicate-from", "peer"]).is_err());
    assert!(parse(&["--replicate-from", "peer:9068/path"]).is_err());
    assert!(parse(&[]).unwrap().replicate.replica().is_none());
    let two =
        ["--replicate-from", "https://snapshots.example/mainnet", "--replicate-from", "peer:9068"];
    let replica = parse(&two).unwrap().replicate.replica().unwrap();
    let sources: Vec<_> = replica.sources.iter().map(ToString::to_string).collect();
    assert_eq!(sources, ["https://snapshots.example/mainnet", "peer:9068"]);
    assert_eq!(replica.sources[1], ReplicaSource::Peer("http://peer:9068".parse().unwrap()));

    let error = run_daemon(parse(&two).unwrap()).unwrap_err();
    assert_eq!(
        error.to_string(),
        "--replicate-from needs --remote-rpc: the node that attests the import"
    );
}
