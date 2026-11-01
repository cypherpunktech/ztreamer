use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};

use serde_json::json;
use tokio::time::advance;

use super::*;
use crate::{
    RpcClient, RpcCode,
    test_utils::{MockNode, Reply},
};

/// A `getblockchaininfo` answer with `blocks` and `headers`.
fn info(blocks: u32, headers: u32) -> GetBlockchainInfoResponse {
    let mut info = serde_json::to_value(GetBlockchainInfoResponse::default()).unwrap();
    info["blocks"] = json!(blocks);
    info["headers"] = json!(headers);
    serde_json::from_value(info).unwrap()
}

#[tokio::test(start_paused = true)]
async fn stalls_when_the_height_does_not_change_for_30_minutes_with_more_headers() {
    let mut stall = StallCheck::default();
    let behind = info(100, 900);
    assert!(!stall.is_stalled(Instant::now(), &behind));

    advance(STALL_LIMIT - Duration::from_secs(1)).await;
    assert!(!stall.is_stalled(Instant::now(), &behind));

    advance(Duration::from_secs(1)).await;
    assert!(stall.is_stalled(Instant::now(), &behind));
}

#[tokio::test(start_paused = true)]
async fn a_new_block_or_no_new_header_is_no_stall() {
    let mut stall = StallCheck::default();
    stall.is_stalled(Instant::now(), &info(100, 900));
    advance(STALL_LIMIT - Duration::from_secs(1)).await;
    assert!(!stall.is_stalled(Instant::now(), &info(101, 900)));
    advance(Duration::from_secs(1)).await;
    assert!(!stall.is_stalled(Instant::now(), &info(101, 900)));

    // The node has no header above its blocks: it waits for its peers, and the wait continues.
    let mut stall = StallCheck::default();
    stall.is_stalled(Instant::now(), &info(100, 100));
    advance(2 * STALL_LIMIT).await;
    assert!(!stall.is_stalled(Instant::now(), &info(100, 100)));
}

/// A node that answers `getblockchaininfo` with [`RpcCode::IN_WARMUP`] for the first
/// `warm_up` requests, then with the height in `height`.
async fn syncing_node(warm_up: usize, height: Arc<AtomicU32>) -> MockNode {
    MockNode::start(move |n, _| {
        if n < warm_up {
            return Reply::Error { code: RpcCode::IN_WARMUP, message: "Loading block index..." };
        }
        let blocks = height.load(Ordering::SeqCst);
        Reply::Result(serde_json::to_value(info(blocks, blocks + 10)).unwrap())
    })
    .await
}

fn wait(mock: &MockNode) -> NodeSyncWait {
    NodeSyncWait::new(ZakuraClient::new(RpcClient::new(mock.config()).unwrap()))
}

#[tokio::test]
async fn waits_through_the_warm_up_and_the_sync() {
    let height = Arc::new(AtomicU32::new(5));
    let mock = syncing_node(2, Arc::clone(&height)).await;
    let sync = tokio::spawn({
        let height = Arc::clone(&height);
        async move {
            // The wait reads the warm-up twice, then height 5, then this height.
            tokio::time::sleep(3 * POLL_INTERVAL + POLL_INTERVAL / 2).await;
            height.store(12, Ordering::SeqCst);
        }
    });

    let info = wait(&mock).wait_for_height(10).await.unwrap();

    assert_eq!(info.blocks().0, 12);
    assert!(mock.seen().len() >= 4, "{}", mock.seen().len());
    sync.await.unwrap();
}

#[tokio::test]
async fn a_node_at_the_target_needs_one_call() {
    let mock = syncing_node(0, Arc::new(AtomicU32::new(10))).await;
    assert_eq!(wait(&mock).wait_for_height(10).await.unwrap().blocks().0, 10);
    assert_eq!(mock.seen().len(), 1);
}

#[tokio::test]
async fn a_gone_or_refusing_node_stops_the_wait() {
    let mock = MockNode::start(|_, _| Reply::Close).await;
    let error = wait(&mock).wait_for_height(10).await.unwrap_err();
    assert!(matches!(error, NodeWaitError::Node { state: NodeState::Gone, .. }), "{error}");
    let url = mock.url().to_string();
    assert!(error.to_string().starts_with(&format!("remote zakura {url} is gone: ")), "{error}");

    let mock =
        MockNode::start(|_, _| Reply::Error { code: RpcCode::METHOD_NOT_FOUND, message: "no" })
            .await;
    let error = wait(&mock).wait_for_height(10).await.unwrap_err();
    assert!(matches!(error, NodeWaitError::Node { state: NodeState::Refuses, .. }), "{error}");
}
