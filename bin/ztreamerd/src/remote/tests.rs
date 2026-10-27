use std::{future, net::SocketAddr, time::Duration};

use clap::Parser;
use serde_json::json;
use tokio::{task::JoinHandle, time::timeout};
use zakura_chain::{
    block::{Block, Height},
    serialization::ZcashDeserializeInto as _,
};
use zakura_test::vectors::BLOCK_TESTNET_GENESIS_BYTES;
use ztreamer_indexer::{index::SEAL_DEPTH, test_utils::grow_chain};
use ztreamer_protocol::proto::BlockId;
use ztreamer_remote::test_utils::{MockChain, MockNode, Reply};

use super::*;
use crate::{
    Cli,
    test_support::{free_addr, latest_block_at, node_with_chain_name},
};

/// The durable tip of the index before the daemon starts.
const INDEX_TIP: u32 = 20;

/// The requests that a warming-up mock answers with [`RpcCode::IN_WARMUP`]. The client sends a
/// call four times before it gives the error, thus the daemon waits at least once.
const WARMUP_REQUESTS: usize = 6;

/// A best chain from mainnet genesis to `tip`.
fn chain(tip: u32) -> Vec<Block> {
    let mut chain = Vec::new();
    grow_chain(&mut chain, tip, 0);
    chain
}

/// The first `tip + 1` blocks of `blocks`.
fn to(blocks: &[Block], tip: u32) -> &[Block] {
    &blocks[..=usize::try_from(tip).unwrap()]
}

/// A mock node that serves `chain`.
async fn serve(chain: &MockChain) -> MockNode {
    let chain = chain.clone();
    MockNode::start(move |_, request| chain.reply(request)).await
}

/// The flags of a daemon on the node at `url` with the index in `dir`, then `extra`.
fn cli(dir: &tempfile::TempDir, url: &str, extra: &[&str]) -> DaemonArgs {
    let dir = dir.path().to_str().unwrap();
    let flags = ["--index-dir", dir, "--index-map-size", "67108864", "--remote-rpc", url];
    let flags = ["--fetch-workers", "2", "--source-segment-blocks", "16"].into_iter().chain(flags);
    Cli::parse_from(["ztreamerd"].into_iter().chain(flags).chain(extra.iter().copied())).daemon
}

/// The node of `cli`, with no default cookie file. Thus a cookie of a local zakura does not
/// change the tests.
fn node(cli: &DaemonArgs) -> RemoteNode {
    cli.remote.node_with_default_cookie(Path::new("/nonexistent/zakura/.cookie")).unwrap().unwrap()
}

/// Runs the daemon with no signal, until it stops by itself.
async fn run(cli: DaemonArgs) -> Result<()> {
    let node = node(&cli);
    let daemon = node.run_until(cli, Server::builder(), future::pending());
    timeout(Duration::from_secs(30), daemon).await.expect("the daemon stops by itself")
}

/// The durable tip of the index in `dir`.
async fn durable_tip(dir: &tempfile::TempDir) -> Option<u32> {
    let index = cli(dir, "http://127.0.0.1:1", &[]).open_index(&Network::Mainnet).await.unwrap();
    index.state().unwrap().durable_tip().map(|tip| tip.height)
}

/// Fills the index in `dir` to [`INDEX_TIP`] from a mock over `blocks`, with `--index-only`.
async fn index(dir: &tempfile::TempDir, blocks: &[Block]) {
    let mock = serve(&MockChain::new(to(blocks, INDEX_TIP + SEAL_DEPTH))).await;
    run(cli(dir, &mock.url().to_string(), &["--index-only"])).await.unwrap();
    assert_eq!(durable_tip(dir).await, Some(INDEX_TIP));
}

/// A daemon that serves gRPC on a free local port until [`Self::stop`] sends the signal.
struct Daemon {
    grpc: SocketAddr,
    signal: CancellationToken,
    task: JoinHandle<Result<()>>,
}

impl Daemon {
    fn start(dir: &tempfile::TempDir, url: &str) -> Self {
        let grpc = free_addr();
        let cli = cli(dir, url, &["--grpc-listen", &grpc.to_string()]);
        let node = node(&cli);
        let signal = CancellationToken::new();
        let received = signal.clone().cancelled_owned();
        let daemon = node.run_until(cli, Server::builder(), async move {
            received.await;
            Ok(())
        });
        Self { grpc, signal, task: tokio::spawn(daemon) }
    }

    /// The answer to `GetLatestBlock`, when the daemon serves `height`.
    async fn latest_block(&self, height: u32) -> BlockId {
        latest_block_at(self.grpc, height, Duration::from_secs(30)).await
    }

    /// Sends the signal. The daemon must stop without an error.
    async fn stop(self) {
        self.signal.cancel();
        timeout(Duration::from_secs(10), self.task)
            .await
            .expect("the daemon stops after the signal")
            .unwrap()
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn serves_an_index_behind_a_mock_chain() {
    let dir = tempfile::tempdir().unwrap();
    // The tail pass writes 50 blocks, the head follower the last `SEAL_DEPTH` blocks.
    let tip = INDEX_TIP + 50 + SEAL_DEPTH;
    let blocks = chain(tip);
    index(&dir, &blocks).await;
    let mock = serve(&MockChain::new(&blocks)).await;

    let daemon = Daemon::start(&dir, &mock.url().to_string());
    let latest = daemon.latest_block(tip).await;
    daemon.stop().await;

    assert_eq!(latest.hash, blocks.last().unwrap().hash().0);
}

/// True when the node got no `getblock` request.
fn read_no_block(mock: &MockNode) -> bool {
    mock.seen().iter().all(|seen| seen.body["method"] != "getblock")
}

#[test]
fn the_chain_name_gives_the_network() {
    let network = |name| Network::try_from(NodeChain(name));
    assert_eq!(network("main").unwrap(), Network::Mainnet);
    assert_eq!(network("test").unwrap(), Network::new_default_testnet());
    let error = network("regtest").unwrap_err();
    assert_eq!(
        error.to_string(),
        r#"remote zakura runs chain "regtest", ztreamerd serves only "main" and "test""#
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn serves_a_testnet_node_with_a_testnet_index() {
    let dir = tempfile::tempdir().unwrap();
    let mut blocks = vec![BLOCK_TESTNET_GENESIS_BYTES.zcash_deserialize_into::<Block>().unwrap()];
    grow_chain(&mut blocks, 10, 0);
    let mock = node_with_chain_name("test", &MockChain::new(&blocks)).await;

    let daemon = Daemon::start(&dir, &mock.url().to_string());
    let latest = daemon.latest_block(10).await;
    daemon.stop().await;

    assert_eq!(latest.hash, blocks[10].hash().0);
    let mainnet = cli(&dir, "http://127.0.0.1:1", &[]).open_index(&Network::Mainnet).await;
    assert!(mainnet.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refuses_an_index_of_another_network() {
    let dir = tempfile::tempdir().unwrap();
    let blocks = chain(INDEX_TIP + SEAL_DEPTH);
    index(&dir, &blocks).await;
    let mock = node_with_chain_name("test", &MockChain::new(&blocks)).await;

    let error = run(cli(&dir, &mock.url().to_string(), &[])).await.unwrap_err();

    let expected = "LMDB network, genesis hash, or fixed index policy does not match";
    assert!(error.chain().any(|cause| cause.to_string() == expected), "{error:?}");
    assert!(read_no_block(&mock));
    assert_eq!(durable_tip(&dir).await, Some(INDEX_TIP));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refuses_a_node_of_an_unknown_chain() {
    let dir = tempfile::tempdir().unwrap();
    let mock = node_with_chain_name("regtest", &MockChain::new(&chain(10))).await;

    let error = run(cli(&dir, &mock.url().to_string(), &[])).await.unwrap_err();

    assert!(error.to_string().contains(r#"chain "regtest""#), "{error:?}");
    assert!(read_no_block(&mock));
    assert!(std::fs::read_dir(dir.path()).unwrap().next().is_none(), "no index is made");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refuses_a_node_with_another_genesis_block() {
    let dir = tempfile::tempdir().unwrap();
    // The chain name is "test", but the genesis block is the one of mainnet.
    let mock = node_with_chain_name("test", &MockChain::new(&chain(10))).await;

    let error = run(cli(&dir, &mock.url().to_string(), &[])).await.unwrap_err();

    let genesis = Network::new_default_testnet().genesis_hash();
    let expected = format!(
        "remote zakura has genesis block {}, the index needs {genesis}",
        Network::Mainnet.genesis_hash()
    );
    assert!(error.chain().any(|cause| cause.to_string() == expected), "{error:?}");
    assert!(read_no_block(&mock));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refuses_an_index_tip_below_the_prune_height() {
    let dir = tempfile::tempdir().unwrap();
    let blocks = chain(INDEX_TIP + 50 + SEAL_DEPTH);
    index(&dir, &blocks).await;
    let chain = MockChain::new(&blocks);
    chain.set_prune_height(Some(Height(50)));
    let mock = serve(&chain).await;

    let error = run(cli(&dir, &mock.url().to_string(), &[])).await.unwrap_err();

    assert_eq!(
        error.to_string(),
        "the index needs block 21, but the remote zakura node keeps block bodies only from height \
         50: use a node that keeps these blocks, or an index with a tip at or above 49"
    );
    assert_eq!(durable_tip(&dir).await, Some(INDEX_TIP));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn waits_for_the_end_of_the_warm_up_then_serves() {
    let dir = tempfile::tempdir().unwrap();
    let blocks = chain(10);
    let chain = MockChain::new(&blocks);
    let mock = MockNode::start(move |n, request| match n {
        ..WARMUP_REQUESTS => {
            Reply::Error { code: RpcCode::IN_WARMUP, message: "Loading block index..." }
        }
        _ => chain.reply(request),
    })
    .await;

    let daemon = Daemon::start(&dir, &mock.url().to_string());
    let latest = daemon.latest_block(10).await;
    daemon.stop().await;

    assert_eq!(latest.hash, blocks[10].hash().0);
    assert!(mock.seen().len() > WARMUP_REQUESTS);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn waits_for_an_unreachable_node_until_the_signal() {
    let dir = tempfile::tempdir().unwrap();
    let mut daemon = Daemon::start(&dir, &format!("http://{}", free_addr()));

    // The client gives up a call after about 1.4 s of retries. The daemon then waits and calls
    // again, thus it is still running after 3 s.
    assert!(timeout(Duration::from_secs(3), &mut daemon.task).await.is_err());
    daemon.stop().await;

    assert_eq!(durable_tip(&dir).await, None);
}

#[test]
fn the_flags_keep_the_two_modes_apart() {
    let parse = |flags: &[&str]| {
        Cli::try_parse_from(["ztreamerd"].iter().chain(flags)).map(|cli| cli.daemon)
    };
    let url = "http://127.0.0.1:8232";
    assert!(parse(&["--remote-rpc", url, "--zakura-config", "zakura.toml"]).is_err());
    assert!(parse(&["--remote-rpc-cookie", ".cookie"]).is_err());
    assert!(parse(&["--remote-rpc", "http://user@127.0.0.1:8232"]).is_err());
    assert!(parse(&[]).unwrap().remote.node().unwrap().is_none());
}

#[test]
fn credentials_in_the_url_become_basic_auth() {
    let url: NodeUrl = "https://us%40er:hunter:2@node.example:8232/".parse().unwrap();
    assert_eq!(url.uri, "https://node.example:8232/");
    let expected = Auth::Basic { user: "us@er".into(), password: Secret::new("hunter:2") };
    assert_eq!(url.credentials, Some(expected));
    assert!(!format!("{url:?}").contains("hunter"));
    assert!(!url.is_loopback());

    let url: NodeUrl = "http://[::1]:8232".parse().unwrap();
    assert_eq!(url.credentials, None);
    assert!(url.is_loopback());
    assert!("http://user@127.0.0.1:8232".parse::<NodeUrl>().is_err());
}

#[test]
fn the_credentials_come_from_the_flag_the_url_or_the_default_cookie() {
    let dir = tempfile::tempdir().unwrap();
    let cookie = dir.path().join(".cookie");
    std::fs::write(&cookie, "__cookie__:secret").unwrap();
    let missing = dir.path().join("missing");
    let auth = |flags: &[&str], default_cookie: &Path| {
        let flags = ["ztreamerd", "--remote-rpc"].iter().chain(flags);
        Cli::parse_from(flags).daemon.remote.auth(default_cookie)
    };
    let flag = ["http://10.0.0.1:8232", "--remote-rpc-cookie", "/zakura/.cookie"];
    assert_eq!(auth(&flag, &cookie).unwrap(), Auth::Cookie("/zakura/.cookie".into()));
    let basic = Auth::Basic { user: "user".into(), password: Secret::new("hunter2") };
    assert_eq!(auth(&["http://user:hunter2@localhost:8232"], &cookie).unwrap(), basic);
    let both = ["http://user:hunter2@localhost:8232", "--remote-rpc-cookie", "/zakura/.cookie"];
    assert!(auth(&both, &cookie).is_err());

    for loopback in ["http://127.0.0.1:8232", "http://localhost:8232", "http://[::1]:8232"] {
        assert_eq!(auth(&[loopback], &cookie).unwrap(), Auth::Cookie(cookie.clone()));
        assert_eq!(auth(&[loopback], &missing).unwrap(), Auth::None);
    }
    // The cookie of a local zakura does not go to another host.
    assert_eq!(auth(&["https://node.example:8232"], &cookie).unwrap(), Auth::None);
}

#[tokio::test]
async fn the_node_gets_the_credentials_of_the_url_and_of_the_default_cookie() {
    let dir = tempfile::tempdir().unwrap();
    let cookie = dir.path().join(".cookie");
    std::fs::write(&cookie, "__cookie__:secret").unwrap();
    let mock = MockNode::start(|_, _| Reply::Result(json!("00"))).await;
    let port = mock.addr().port();
    let headers = async |url: String| {
        let cli = Cli::parse_from(["ztreamerd", "--remote-rpc", &url]).daemon;
        let node = cli.remote.node_with_default_cookie(&cookie).unwrap().unwrap();
        assert!(!format!("{node:?}").contains("hunter2"));
        node.client.rpc().call::<_, String>("getbestblockhash", ()).await.unwrap();
        mock.seen().pop().unwrap().authorization
    };

    let basic = headers(format!("http://us%40er:hunter2@127.0.0.1:{port}")).await;
    assert_eq!(basic.as_deref(), Some("Basic dXNAZXI6aHVudGVyMg=="));
    let cookie = headers(format!("http://127.0.0.1:{port}")).await;
    assert_eq!(cookie.as_deref(), Some("Basic X19jb29raWVfXzpzZWNyZXQ="));
}
