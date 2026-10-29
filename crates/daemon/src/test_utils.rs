//! Test fixtures for the daemon tests: the configuration of a daemon in the remote mode, a free
//! local address, a poll of `GetLatestBlock`, and a mock node with another chain name.

use std::{
    net::{SocketAddr, TcpListener},
    num::NonZeroUsize,
    path::Path,
    time::Duration,
};

use serde_json::json;
use tokio::time::timeout;
use ztreamer_protocol::proto::{
    BlockId, ChainSpec, compact_tx_streamer_client::CompactTxStreamerClient,
};
use ztreamer_remote::test_utils::{MockChain, MockNode, Reply};
use ztreamer_sync::pipeline::PipelineConfig;

use crate::{DaemonConfig, IndexConfig, Mode, RemoteConfig, ServerConfig, SyncConfig};

/// The configuration of a daemon in the remote mode on the node at `url`, with a 64 MiB index in
/// `dir` and the import of `replicate_from`. The other values are the defaults of `ztreamerd`.
pub(crate) fn remote_config(dir: &Path, url: &str, replicate_from: &str) -> DaemonConfig {
    let remote =
        RemoteConfig::new(url.parse().unwrap(), None, replicate_from.parse().unwrap()).unwrap();
    DaemonConfig {
        index: IndexConfig { dir: dir.to_owned(), map_size: NonZeroUsize::new(64 << 20).unwrap() },
        sync: SyncConfig { pipeline: PipelineConfig::default(), index_only: false },
        servers: ServerConfig {
            grpc_listen: "127.0.0.1:9067".parse().unwrap(),
            tls: None,
            metrics_listen: "127.0.0.1:9999".parse().unwrap(),
            ping_very_insecure: false,
        },
        mode: Mode::Remote(Box::new(remote)),
    }
}

/// The remote configuration of `config`. Panics in the embedded mode.
pub(crate) fn remote_mode_config(config: &DaemonConfig) -> &RemoteConfig {
    let Mode::Remote(remote) = &config.mode else { panic!("not the remote mode") };
    remote
}

/// A local address with no listener.
pub(crate) fn free_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap()
}

/// The answer to `GetLatestBlock` from the gRPC server at `grpc`, when it serves `height`. Polls
/// every 50 ms, and panics after `wait`.
pub(crate) async fn latest_block_at(grpc: SocketAddr, height: u32, wait: Duration) -> BlockId {
    let poll = async {
        loop {
            if let Ok(mut client) = CompactTxStreamerClient::connect(format!("http://{grpc}")).await
                && let Ok(latest) = client.get_latest_block(ChainSpec {}).await
                && latest.get_ref().height == u64::from(height)
            {
                return latest.into_inner();
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    timeout(wait, poll).await.expect("the daemon serves the tip")
}

/// A mock node that serves `chain` and gives `name` as its chain name.
pub(crate) async fn node_with_chain_name(name: &'static str, chain: &MockChain) -> MockNode {
    let chain = chain.clone();
    MockNode::start(move |_, request| match chain.reply(request) {
        Reply::Result(mut info) if request.body["method"] == "getblockchaininfo" => {
            info["chain"] = json!(name);
            Reply::Result(info)
        }
        reply => reply,
    })
    .await
}
