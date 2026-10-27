//! Test fixtures for the daemon tests of the remote node and of the replica: a free local
//! address, a poll of `GetLatestBlock`, and a mock node with another chain name.

use std::{
    net::{SocketAddr, TcpListener},
    time::Duration,
};

use serde_json::json;
use tokio::time::timeout;
use ztreamer_protocol::proto::{
    BlockId, ChainSpec, compact_tx_streamer_client::CompactTxStreamerClient,
};
use ztreamer_remote::test_utils::{MockChain, MockNode, Reply};

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
