use std::time::Duration;

use crate::{
    RpcClient, RpcCode,
    test_utils::{MockChain, MockNode, Reply},
};
use serde_json::json;
use tokio::sync::watch;
use zakura_chain::{block::Block, serialization::ZcashSerialize as _};
use ztreamer_chain::test_utils::grow_chain;
use ztreamer_sync::{
    head::{HeadFollower, HeadFollowerConfig},
    pipeline::PipelineConfig,
};

use super::*;
use ztreamer_service::test_utils::EphemeralService;

/// A mock node that serves `chain`, and a source over it.
async fn node(chain: &MockChain) -> (MockNode, RemoteHeadSource) {
    let mock = MockNode::start({
        let chain = chain.clone();
        move |_, request| chain.reply(request)
    })
    .await;
    let source = RemoteHeadSource::new(ZakuraClient::new(RpcClient::new(mock.config()).unwrap()));
    (mock, source)
}

fn chain(tip: u32) -> Vec<Block> {
    let mut chain = Vec::new();
    grow_chain(&mut chain, tip, 0);
    chain
}

fn visible_tip(follower: &HeadFollower) -> Option<BlockId> {
    follower.snapshot().visible_tip
}

fn id(block: &Block) -> BlockId {
    BlockId::new(block.coinbase_height().unwrap().0, block.hash())
}

#[tokio::test]
async fn reads_the_tip_and_the_blocks() {
    let blocks = chain(5);
    let (_mock, mut source) = node(&MockChain::new(&blocks)).await;

    assert_eq!(source.tip().await.unwrap(), Some(id(&blocks[5])));
    let block = source.block(3).await.unwrap().unwrap();
    assert_eq!(block.height, block::Height(3));
    assert_eq!(block.hash, blocks[3].hash());
    assert_eq!(block.bytes, blocks[3].zcash_serialize_to_vec().unwrap());
    assert_eq!(block.txids, [blocks[3].transactions[0].hash()]);
    assert!(source.block(6).await.unwrap().is_none());
}

#[tokio::test]
async fn a_height_above_the_tip_of_the_node_is_no_block() {
    let blocks = chain(5);
    let chain = MockChain::new(&blocks);
    // This error has the code of the `getblockhash` error above the tip, with another message.
    let mock = MockNode::start(move |_, request| {
        match (request.body["method"].as_str(), request.body["params"][0].as_u64()) {
            (Some("getblockhash"), Some(7)) => {
                Reply::Error { code: RpcCode::MISC, message: "Invalid params" }
            }
            _ => chain.reply(request),
        }
    })
    .await;
    let mut source =
        RemoteHeadSource::new(ZakuraClient::new(RpcClient::new(mock.config()).unwrap()));

    assert!(source.block(5).await.unwrap().is_some());
    assert!(source.block(6).await.unwrap().is_none());
    assert_eq!(source.block_hash(5).await.unwrap(), Some(blocks[5].hash()));
    assert_eq!(source.block_hash(6).await.unwrap(), None);
    assert!(matches!(source.block_hash(7).await, Err(HeadError::Remote(_))));
}

#[tokio::test]
async fn a_block_at_another_height_is_refused() {
    let blocks = chain(5);
    let sent = blocks[4].clone();
    let chain = MockChain::new(&blocks);
    let mock = MockNode::start(move |_, request| match request.body["params"][0].as_str() {
        Some("3") => Reply::Result(json!(hex::encode(sent.zcash_serialize_to_vec().unwrap()))),
        _ => chain.reply(request),
    })
    .await;
    let mut source =
        RemoteHeadSource::new(ZakuraClient::new(RpcClient::new(mock.config()).unwrap()));

    let result = source.block(3).await;

    assert!(matches!(result, Err(HeadError::Height { expected: 3, actual: 4 })), "{result:?}");
    assert!(source.block(4).await.unwrap().is_some());
}

#[tokio::test]
async fn sync_head_reaches_a_moving_tip_and_replaces_a_reorged_head() {
    let ctx = EphemeralService::empty().await;
    let mut blocks = chain(20);
    let chain = MockChain::new(&blocks);
    let (_mock, mut source) = node(&chain).await;

    ctx.follower.sync_head(&mut source, PipelineConfig::default()).await.unwrap();
    assert_eq!(visible_tip(&ctx.follower), Some(id(&blocks[20])));

    grow_chain(&mut blocks, 25, 0);
    chain.set_blocks(&blocks);
    let state = ctx.follower.sync_head(&mut source, PipelineConfig::default()).await.unwrap();
    assert_eq!(visible_tip(&ctx.follower), Some(id(&blocks[25])));
    assert_eq!(state.durable_tip().unwrap().height, 15);

    // A reorg above the durable tip replaces only the volatile head.
    let mut fork = blocks[..=21].to_vec();
    grow_chain(&mut fork, 26, 1);
    chain.set_blocks(&fork);
    let next = ctx.follower.sync_head(&mut source, PipelineConfig::default()).await.unwrap();
    let snapshot = ctx.follower.snapshot();
    assert_eq!(snapshot.visible_tip, Some(id(&fork[26])));
    assert_eq!(next.durable_tip().unwrap().height, 16);
    assert!(
        snapshot
            .volatile_head
            .iter()
            .all(|record| record.hash == fork[record.height as usize].hash())
    );
    assert_ne!(fork[22].hash(), blocks[22].hash());
    ctx.index.latest().unwrap().verify_continuity().unwrap();
}

#[tokio::test]
async fn follow_head_reaches_a_moving_tip() {
    let ctx = EphemeralService::empty().await;
    let mut blocks = chain(12);
    let chain = MockChain::new(&blocks);
    let (_mock, source) = node(&chain).await;
    let (shutdown, receiver) = watch::channel(false);
    let follower = tokio::spawn({
        let follower = ctx.follower.clone();
        async move {
            follower
                .follow_head(
                    source,
                    PipelineConfig::default(),
                    HeadFollowerConfig {
                        poll_interval: Duration::from_millis(1),
                        attempt_timeout: Duration::from_secs(1),
                        freshness_timeout: Duration::from_secs(1),
                    },
                    receiver,
                )
                .await
        }
    });
    let reaches = |tip: BlockId| {
        let follower = ctx.follower.clone();
        tokio::time::timeout(Duration::from_secs(5), async move {
            while visible_tip(&follower) != Some(tip) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
    };

    reaches(id(&blocks[12])).await.unwrap();
    grow_chain(&mut blocks, 30, 0);
    chain.set_blocks(&blocks);
    reaches(id(&blocks[30])).await.unwrap();

    let mut fork = blocks[..=25].to_vec();
    grow_chain(&mut fork, 31, 1);
    chain.set_blocks(&fork);
    reaches(id(&fork[31])).await.unwrap();
    assert!(ctx.follower.readiness().tip);

    shutdown.send(true).unwrap();
    follower.await.unwrap().unwrap();
}

/// The requests that `mock` received after the first `from` requests, and how many of them
/// are `getblock`.
fn requests_since(mock: &MockNode, from: usize) -> (usize, usize) {
    let seen = &mock.seen()[from..];
    (seen.len(), seen.iter().filter(|request| request.body["method"] == "getblock").count())
}

#[tokio::test]
async fn a_tip_change_costs_requests_for_the_new_blocks_only() {
    let ctx = EphemeralService::empty().await;
    let mut blocks = chain(150);
    let chain = MockChain::new(&blocks);
    let (mock, mut source) = node(&chain).await;
    ctx.follower.sync_head(&mut source, PipelineConfig::default()).await.unwrap();

    for (tip, new) in [(151, 1), (154, 3)] {
        grow_chain(&mut blocks, tip, 0);
        chain.set_blocks(&blocks);
        let before = mock.seen().len();
        ctx.follower.sync_head(&mut source, PipelineConfig::default()).await.unwrap();

        // The poll sends one `getblockchaininfo` and three `getblockhash`: the anchor, the old
        // tip and the re-check of the new tip. It sends one `getblock` for each new block and
        // one `getblock` for the height above the new tip.
        assert_eq!(requests_since(&mock, before), (5 + new, new + 1));
        assert_eq!(visible_tip(&ctx.follower), Some(id(&blocks[tip as usize])));
    }
}
