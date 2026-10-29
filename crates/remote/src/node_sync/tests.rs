use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};

use serde_json::json;
use tokio::time::advance;

use super::{
    progress::{NodeHeights, ProgressLine},
    *,
};
use crate::{
    RpcClient, RpcCode,
    test_utils::{MockNode, Reply},
};

/// A `getblockchaininfo` answer with `blocks` and `headers`, and an estimated height of 1,000.
fn info(blocks: u32, headers: u32) -> GetBlockchainInfoResponse {
    let mut info = serde_json::to_value(GetBlockchainInfoResponse::default()).unwrap();
    info["blocks"] = json!(blocks);
    info["headers"] = json!(headers);
    info["estimatedheight"] = json!(1_000);
    info["verificationprogress"] = json!(0.5);
    serde_json::from_value(info).unwrap()
}

fn heights(blocks: u32, headers: u32) -> Option<NodeHeights> {
    Some(NodeHeights::from(&info(blocks, headers)))
}

#[tokio::test(start_paused = true)]
async fn logs_the_rate_and_the_eta_each_minute() {
    let mut progress = SyncProgress::new(1_000);
    let first = progress.observe(Instant::now(), Reading::Behind(&info(100, 1_000))).unwrap();
    let expected = ProgressLine { target: 1_000, node: heights(100, 1_000), rate: None, eta: None };
    assert_eq!(first, Some(expected));

    advance(Duration::from_secs(30)).await;
    let half = progress.observe(Instant::now(), Reading::Behind(&info(130, 1_000))).unwrap();
    assert_eq!(half, None);

    advance(Duration::from_secs(30)).await;
    let minute = progress.observe(Instant::now(), Reading::Behind(&info(160, 1_000))).unwrap();
    let expected = ProgressLine {
        target: 1_000,
        node: heights(160, 1_000),
        rate: Some(1.0),
        eta: Some(Duration::from_secs(840)),
    };
    assert_eq!(minute, Some(expected));

    // No block in the next minute: the rate is zero, and there is no ETA.
    advance(LOG_INTERVAL).await;
    let line = progress.observe(Instant::now(), Reading::Behind(&info(160, 1_000))).unwrap();
    assert_eq!(line.map(|line| (line.rate, line.eta)), Some((Some(0.0), None)));
}

#[tokio::test(start_paused = true)]
async fn logs_a_warm_up_with_no_height() {
    let mut progress = SyncProgress::new(10);
    let line = progress.observe(Instant::now(), Reading::WarmUp).unwrap();
    assert_eq!(line, Some(ProgressLine { target: 10, node: None, rate: None, eta: None }));

    advance(LOG_INTERVAL).await;
    let line = progress.observe(Instant::now(), Reading::Behind(&info(5, 10))).unwrap();
    assert_eq!(line.map(|line| line.rate), Some(None), "no rate from a warm-up line");
}

#[tokio::test(start_paused = true)]
async fn stops_when_the_height_does_not_change_for_30_minutes_with_more_headers() {
    let mut progress = SyncProgress::new(1_000);
    let behind = info(100, 900);
    progress.observe(Instant::now(), Reading::Behind(&behind)).unwrap();

    advance(STALL_LIMIT - Duration::from_secs(1)).await;
    assert!(progress.observe(Instant::now(), Reading::Behind(&behind)).is_ok());
    // A warm-up answer does not reset the time of the last height change.
    assert!(progress.observe(Instant::now(), Reading::WarmUp).is_ok());

    advance(Duration::from_secs(1)).await;
    let stall = progress.observe(Instant::now(), Reading::Behind(&behind)).unwrap_err();
    assert_eq!(stall, Stall { height: 100, headers: 900 });
}

#[tokio::test(start_paused = true)]
async fn a_new_block_or_no_new_header_is_no_stall() {
    let mut progress = SyncProgress::new(1_000);
    progress.observe(Instant::now(), Reading::Behind(&info(100, 900))).unwrap();
    advance(STALL_LIMIT - Duration::from_secs(1)).await;
    progress.observe(Instant::now(), Reading::Behind(&info(101, 900))).unwrap();
    advance(Duration::from_secs(1)).await;
    assert!(progress.observe(Instant::now(), Reading::Behind(&info(101, 900))).is_ok());

    // The node has no header above its blocks: it waits for its peers, and the wait continues.
    let mut progress = SyncProgress::new(1_000);
    progress.observe(Instant::now(), Reading::Behind(&info(100, 100))).unwrap();
    advance(2 * STALL_LIMIT).await;
    assert!(progress.observe(Instant::now(), Reading::Behind(&info(100, 100))).is_ok());
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
