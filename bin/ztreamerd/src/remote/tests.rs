use std::{future, time::Duration};

use clap::{Parser, error::ErrorKind};
use serde_json::json;
use tokio::{task::JoinHandle, time::timeout};
use zakura_chain::{
    block::{Block, Height},
    serialization::ZcashDeserializeInto as _,
};
use zakura_test::vectors::BLOCK_TESTNET_GENESIS_BYTES;
use ztreamer_indexer::test_utils::grow_chain;
use ztreamer_remote::test_utils::{MockChain, MockNode, Reply};

use super::*;
use crate::{
    Cli,
    test_support::{free_addr, node_with_chain_name},
};

/// A `--replicate-from` source that no server serves. The tests here stop before the import
/// fetches from it, or test that the import fails.
const NO_SNAPSHOT: &str = "http://127.0.0.1:1/snapshot";

/// The requests that a warming-up mock answers with [`RpcCode::IN_WARMUP`]. The client sends a
/// call four times before it gives the error, thus the daemon waits at least once.
const WARMUP_REQUESTS: usize = 6;

/// A best chain from mainnet genesis to `tip`.
fn chain(tip: u32) -> Vec<Block> {
    let mut chain = Vec::new();
    grow_chain(&mut chain, tip, 0);
    chain
}

/// The flags of a daemon on the node at `url` with the index in `dir` and the import of
/// [`NO_SNAPSHOT`], then `extra`.
fn cli(dir: &tempfile::TempDir, url: &str, extra: &[&str]) -> DaemonArgs {
    let dir = dir.path().to_str().unwrap();
    let flags = ["--index-dir", dir, "--index-map-size", "67108864", "--remote-rpc", url];
    let flags = flags.into_iter().chain(["--replicate-from", NO_SNAPSHOT]);
    Cli::parse_from(["ztreamerd"].into_iter().chain(flags).chain(extra.iter().copied())).daemon
}

/// The node of `cli`, with no default cookie file. Thus a cookie of a local zakura does not
/// change the tests.
fn node(cli: &DaemonArgs) -> RemoteNode {
    let client = cli.remote.client(Path::new("/nonexistent/zakura/.cookie")).unwrap().unwrap();
    RemoteNode::new(client, cli.replicate.replica())
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

/// A daemon on a free gRPC port until [`Self::stop`] sends the signal.
struct Daemon {
    signal: CancellationToken,
    task: JoinHandle<Result<()>>,
}

impl Daemon {
    fn start(dir: &tempfile::TempDir, url: &str) -> Self {
        let cli = cli(dir, url, &["--grpc-listen", &free_addr().to_string()]);
        let node = node(&cli);
        let signal = CancellationToken::new();
        let received = signal.clone().cancelled_owned();
        let daemon = node.run_until(cli, Server::builder(), async move {
            received.await;
            Ok(())
        });
        Self { signal, task: tokio::spawn(daemon) }
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
async fn a_testnet_node_gives_a_testnet_index_before_the_import() {
    let dir = tempfile::tempdir().unwrap();
    let mut blocks = vec![BLOCK_TESTNET_GENESIS_BYTES.zcash_deserialize_into::<Block>().unwrap()];
    grow_chain(&mut blocks, 10, 0);
    let mock = node_with_chain_name("test", &MockChain::new(&blocks)).await;

    // The import of `NO_SNAPSHOT` fails after the daemon opens the index.
    let error = run(cli(&dir, &mock.url().to_string(), &[])).await.unwrap_err();

    assert_eq!(error.to_string(), "import the sealed ranges");
    let open = async |network| cli(&dir, NO_SNAPSHOT, &[]).open_index(&network).await;
    assert!(open(Network::new_default_testnet()).await.is_ok());
    assert!(open(Network::Mainnet).await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refuses_an_index_of_another_network() {
    let dir = tempfile::tempdir().unwrap();
    drop(cli(&dir, NO_SNAPSHOT, &[]).open_index(&Network::Mainnet).await.unwrap());
    let mock = node_with_chain_name("test", &MockChain::new(&chain(10))).await;

    let error = run(cli(&dir, &mock.url().to_string(), &[])).await.unwrap_err();

    let expected = "LMDB network, genesis hash, or fixed index policy does not match";
    assert!(error.chain().any(|cause| cause.to_string() == expected), "{error:?}");
    assert!(read_no_block(&mock));
    assert_eq!(durable_tip(&dir).await, None);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refuses_a_node_that_pruned_the_next_block() {
    let dir = tempfile::tempdir().unwrap();
    let chain = MockChain::new(&chain(10));
    let mock = MockNode::start(move |_, request| match chain.reply(request) {
        Reply::Result(mut info) if request.body["method"] == "getblockchaininfo" => {
            info["pruned"] = json!(true);
            info["pruneheight"] = json!(5);
            Reply::Result(info)
        }
        reply => reply,
    })
    .await;

    let error = run(cli(&dir, &mock.url().to_string(), &[])).await.unwrap_err();

    let expected = "the import needs block 0, but the remote zakura node keeps block bodies only \
                    from height 5: use a node that does not prune, or an index that is sealed \
                    through height 4 or above";
    assert!(error.chain().any(|cause| cause.to_string() == expected), "{error:?}");
    assert!(read_no_block(&mock));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn waits_for_the_end_of_the_warm_up() {
    let dir = tempfile::tempdir().unwrap();
    let chain = MockChain::new(&chain(10));
    let mock = MockNode::start(move |n, request| match n {
        ..WARMUP_REQUESTS => {
            Reply::Error { code: RpcCode::IN_WARMUP, message: "Loading block index..." }
        }
        _ => chain.reply(request),
    })
    .await;
    let node = node(&cli(&dir, &mock.url().to_string(), &[]));

    let shutdown = CancellationToken::new();
    let info = node.blockchain_info_when_up(&shutdown);
    let info = timeout(Duration::from_secs(30), info).await.unwrap().unwrap();

    let ControlFlow::Continue(info) = info else { panic!("no shutdown was sent") };
    assert_eq!(info.blocks(), Height(10));
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
    let replica = ["--replicate-from", "peer:9068"];
    let remote = |url| ["--remote-rpc", url].into_iter().chain(replica).collect::<Vec<_>>();
    let url = "http://127.0.0.1:8232";
    let remote_and_embedded = [remote(url), vec!["--zakura-config", "zakura.toml"]].concat();
    assert!(parse(&remote_and_embedded).is_err());
    assert!(parse(&["--remote-rpc-cookie", ".cookie"]).is_err());
    assert!(parse(&remote("http://user@127.0.0.1:8232")).is_err());
    let embedded = parse(&[]).unwrap();
    assert!(embedded.remote.node(&embedded.replicate).unwrap().is_none());
    let remote = parse(&remote(url)).unwrap();
    assert!(remote.remote.node(&remote.replicate).unwrap().is_some());
}

#[test]
fn the_remote_mode_needs_both_remote_rpc_and_replicate_from() {
    let missing = |flags: &[&str]| {
        let error = Cli::try_parse_from(["ztreamerd"].iter().chain(flags)).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::MissingRequiredArgument, "{error}");
        error.to_string()
    };
    let error = missing(&["--remote-rpc", "http://127.0.0.1:8232"]);
    assert!(error.contains("--replicate-from <URL|HOST:PORT>"), "{error}");
    let error = missing(&["--replicate-from", "https://snapshots.example/mainnet"]);
    assert!(error.contains("--remote-rpc <URL>"), "{error}");
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
        let flags =
            ["ztreamerd", "--replicate-from", "peer:9068", "--remote-rpc"].iter().chain(flags);
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
        let flags = ["ztreamerd", "--remote-rpc", &url, "--replicate-from", "peer:9068"];
        let client = Cli::parse_from(flags).daemon.remote.client(&cookie).unwrap().unwrap();
        assert!(!format!("{client:?}").contains("hunter2"));
        client.rpc().call::<_, String>("getbestblockhash", ()).await.unwrap();
        mock.seen().pop().unwrap().authorization
    };

    let basic = headers(format!("http://us%40er:hunter2@127.0.0.1:{port}")).await;
    assert_eq!(basic.as_deref(), Some("Basic dXNAZXI6aHVudGVyMg=="));
    let cookie = headers(format!("http://127.0.0.1:{port}")).await;
    assert_eq!(cookie.as_deref(), Some("Basic X19jb29raWVfXzpzZWNyZXQ="));
}
