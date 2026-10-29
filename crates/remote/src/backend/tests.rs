use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicBool, Ordering},
};

use super::*;
use crate::{
    RpcClient, RpcCode,
    test_utils::{MockNode, Reply},
};
use serde_json::{Value, json};
use zakura_rpc::client::{
    Commitments, GetBlockchainInfoResponse, GetTreestateResponse, HexData, TransactionObject, Utxo,
};
use zakura_test::vectors::{BLOCK_MAINNET_1_BYTES, BLOCK_MAINNET_2_BYTES};

/// The mainnet answers that `connect` and the tip reads need, for a node with tip `tip`.
fn chain_reply(method: &str, tip: u32) -> Option<Reply> {
    Some(match method {
        "getblockchaininfo" => {
            let mut info = serde_json::to_value(GetBlockchainInfoResponse::default()).unwrap();
            info["blocks"] = json!(tip);
            Reply::Result(info)
        }
        "getblockcount" => Reply::Result(json!(tip)),
        "getblockhash" => Reply::Result(json!(Network::Mainnet.genesis_hash().to_string())),
        _ => return None,
    })
}

/// A mock mainnet node with tip `tip`. It answers other methods from `answer`.
async fn mainnet(
    tip: u32,
    answer: impl Fn(&str, &Value) -> Reply + Send + Sync + 'static,
) -> MockNode {
    MockNode::start(move |_, seen| {
        let (method, params) = (seen.body["method"].as_str().unwrap(), &seen.body["params"]);
        chain_reply(method, tip).unwrap_or_else(|| answer(method, params))
    })
    .await
}

async fn backend(mock: &MockNode) -> RemoteBackend {
    let node = ZakuraClient::new(RpcClient::new(mock.config()).unwrap());
    RemoteBackend::connect(node, Network::Mainnet).await.unwrap()
}

fn block_1() -> Block {
    BLOCK_MAINNET_1_BYTES.zcash_deserialize_into().unwrap()
}

fn orchard_root(seed: u8) -> orchard::tree::Node {
    orchard::tree::Node::try_from([seed; 32].map(|byte| byte & 0x0f)).unwrap()
}

#[tokio::test]
async fn connect_checks_the_network() {
    let mock = mainnet(1, |_, _| Reply::Close).await;
    let node = ZakuraClient::new(RpcClient::new(mock.config()).unwrap());
    let backend = RemoteBackend::connect(node.clone(), Network::Mainnet).await.unwrap();
    assert_eq!(backend.network(), Network::Mainnet);
    assert_eq!(backend.tip_height().await.unwrap(), Some(block::Height(1)));

    let error =
        RemoteBackend::connect(node, Network::new_regtest(Default::default())).await.unwrap_err();
    assert!(
        matches!(&error, RemoteConnectError::WrongChain { expected, found }
                if expected == "test" && found == "main"),
        "{error}"
    );
}

#[tokio::test]
async fn connect_checks_the_genesis_hash() {
    let mock = MockNode::start(|_, seen| match seen.body["method"].as_str().unwrap() {
        "getblockchaininfo" => {
            Reply::Result(serde_json::to_value(GetBlockchainInfoResponse::default()).unwrap())
        }
        _ => Reply::Result(json!(block::Hash([7; 32]).to_string())),
    })
    .await;
    let node = ZakuraClient::new(RpcClient::new(mock.config()).unwrap());
    let error = RemoteBackend::connect(node, Network::Mainnet).await.unwrap_err();
    assert!(
        matches!(error, RemoteConnectError::WrongGenesis { found, .. } if found == block::Hash([7; 32]))
    );
}

#[tokio::test]
async fn block_parses_the_raw_answer() {
    let mock = mainnet(1, |_, params| match params[0].as_str().unwrap() {
        "1111111111111111111111111111111111111111111111111111111111111111" => {
            Reply::Error { code: RpcCode::INVALID_PARAMETER, message: "Block not found" }
        }
        _ => Reply::Result(serde_json::to_value(HexData(BLOCK_MAINNET_1_BYTES.clone())).unwrap()),
    })
    .await;
    let backend = backend(&mock).await;
    let expected = block_1();
    let block = backend.block(expected.hash()).await.unwrap().unwrap();
    assert_eq!(*block, expected);
    assert!(backend.block(block::Hash([0x11; 32])).await.unwrap().is_none());
    let error = backend.block(block::Hash([0; 32])).await.unwrap_err();
    assert!(matches!(error, BackendError::Unexpected(_)), "{error}");
}

#[tokio::test]
async fn transaction_parses_the_verbose_answer() {
    let coinbase = block_1().transactions[0].clone();
    let id = coinbase.hash();
    let mined = Arc::new(AtomicBool::new(true));
    let in_chain = mined.clone();
    let mock = mainnet(1, move |_, _| {
        let height = in_chain.load(Ordering::SeqCst).then_some(block::Height(1));
        Reply::Result(transaction_object(&coinbase, height))
    })
    .await;
    let backend = backend(&mock).await;
    let found = backend.transaction(id).await.unwrap().unwrap();
    assert_eq!(found.transaction.hash(), id);
    assert_eq!(found.height, block::Height(1));
    // A mempool transaction is not in the best chain.
    mined.store(false, Ordering::SeqCst);
    assert!(backend.transaction(id).await.unwrap().is_none());
    // A mined answer for another id is refused.
    mined.store(true, Ordering::SeqCst);
    let error = backend.transaction(transaction::Hash([1; 32])).await.unwrap_err();
    assert!(matches!(error, BackendError::Unexpected(_)), "{error}");
}

/// The verbose `getrawtransaction` answer for `transaction`, mined at `height` or in the
/// mempool.
fn transaction_object(transaction: &Arc<Transaction>, height: Option<block::Height>) -> Value {
    serde_json::to_value(TransactionObject::from_transaction(
        transaction.clone(),
        height,
        height.map(|_| 1),
        &Network::Mainnet,
        None,
        None,
        Some(height.is_some()),
        transaction.hash(),
    ))
    .unwrap()
}

#[tokio::test]
async fn subtrees_parse_each_pool_in_stored_byte_order() {
    let sapling = SaplingNode::from_bytes([3; 32]).unwrap();
    let orchard = orchard_root(5);
    let mock = mainnet(2_000_000, move |_, params| {
        let root = match params[0].as_str().unwrap() {
            "sapling" => hex::encode(sapling.to_bytes()),
            _ => hex::encode(orchard.to_repr()),
        };
        Reply::Result(json!({
            "pool": params[0],
            "start_index": params[1],
            "subtrees": [{"root": root, "end_height": 1_000_000}],
        }))
    })
    .await;
    let backend = backend(&mock).await;
    let index = NoteCommitmentSubtreeIndex(4);
    fn expected<N>(root: N) -> Subtrees<N> {
        let subtree = NoteCommitmentSubtreeData::new(block::Height(1_000_000), root);
        BTreeMap::from([(NoteCommitmentSubtreeIndex(4), subtree)])
    }
    assert_eq!(backend.sapling_subtrees(index, None).await.unwrap(), expected(sapling));
    assert_eq!(backend.orchard_subtrees(index, None).await.unwrap(), expected(orchard));
    assert_eq!(backend.ironwood_subtrees(index, None).await.unwrap(), expected(orchard));
}

#[tokio::test]
async fn subtrees_stop_at_the_last_index() {
    let root = hex::encode(orchard_root(1).to_repr());
    let mock = mainnet(10_000, move |_, params| {
        let subtree = json!({"root": root, "end_height": 100});
        Reply::Result(
            json!({"pool": "orchard", "start_index": params[1], "subtrees": [subtree, subtree]}),
        )
    })
    .await;
    let backend = backend(&mock).await;
    let last = NoteCommitmentSubtreeIndex(u16::MAX);
    let error = backend.orchard_subtrees(last, None).await.unwrap_err();
    assert!(matches!(&error, BackendError::Unexpected(message) if message.contains("65535")));
}

fn tree_state_answer(hash: block::Hash, orchard_root: Option<Vec<u8>>) -> Value {
    let sapling: sapling::tree::NoteCommitmentTree = (1..=3u8)
        .map(|i| {
            sapling::tree::NoteCommitmentUpdate::from_bytes(&[i; 32].map(|b| b & 0x0f)).unwrap()
        })
        .collect::<Vec<_>>()
        .into();
    let orchard: orchard::tree::NoteCommitmentTree =
        (1..=5).map(orchard::tree::NoteCommitmentUpdate::from).collect::<Vec<_>>().into();
    let orchard_root = orchard_root.unwrap_or(orchard.root().bytes_in_display_order().to_vec());
    let commitments =
        |root: Vec<u8>, tree| Treestate::new(Commitments::new(Some(root), Some(tree)));
    serde_json::to_value(GetTreestateResponse::new(
        hash,
        block::Height(1),
        0,
        None,
        commitments(sapling.root().bytes_in_display_order().to_vec(), sapling.to_rpc_bytes()),
        commitments(orchard_root, orchard.to_rpc_bytes()),
        Treestate::default(),
    ))
    .unwrap()
}

#[tokio::test]
async fn tree_state_parses_once_per_hash() {
    let (good, bad_root) = (block::Hash([1; 32]), block::Hash([2; 32]));
    let mock = mainnet(1, move |_, params| match params[0].as_str().unwrap() {
        hash if hash == good.to_string() => Reply::Result(tree_state_answer(good, None)),
        hash if hash == bad_root.to_string() => {
            Reply::Result(tree_state_answer(bad_root, Some(vec![0; 32])))
        }
        _ => Reply::Result(tree_state_answer(good, None)),
    })
    .await;
    let backend = backend(&mock).await;
    let trees = backend.tree_state(good).await.unwrap();
    assert_eq!(trees.sapling.unwrap().count(), 3);
    assert_eq!(trees.orchard.unwrap().count(), 5);
    assert!(trees.ironwood.is_none());
    assert_eq!(backend.lock_tree_states().len(), 1);
    let requests = mock.seen().len();
    assert_eq!(backend.tree_state(good).await.unwrap().orchard.unwrap().count(), 5);
    assert_eq!(mock.seen().len(), requests);

    for hash in [bad_root, block::Hash([3; 32])] {
        let error = backend.tree_state(hash).await.unwrap_err();
        assert!(matches!(error, BackendError::Unexpected(_)), "{error}");
    }
}

#[tokio::test]
async fn tree_state_cache_keeps_at_most_its_cap() {
    let mock = mainnet(1, |_, _| Reply::Close).await;
    let backend = backend(&mock).await;
    let hash = |index: u32| {
        let mut bytes = [0; 32];
        bytes[..4].copy_from_slice(&index.to_le_bytes());
        block::Hash(bytes)
    };
    let cap = TREE_CACHE_ENTRIES.get() as u32;
    let mut cache = backend.lock_tree_states();
    for index in 0..cap {
        cache.put(hash(index), TreeState::default());
    }
    // A hit makes hash 0 the most recent entry, so the next insert drops hash 1.
    assert!(cache.get(&hash(0)).is_some());
    cache.put(hash(cap), TreeState::default());
    assert_eq!(cache.len(), cap as usize);
    assert!(cache.contains(&hash(0)) && cache.contains(&hash(cap)));
    assert!(!cache.contains(&hash(1)));
}

const ADDRESS: &str = "t1at7nVNsv6taLRrNRvnQdtfLNRDfsGc3Ak";

fn address() -> transparent::Address {
    ADDRESS.parse().unwrap()
}

/// The params of the last request of the mock.
fn last_params(mock: &MockNode) -> Value {
    mock.seen().last().unwrap().body["params"].clone()
}

#[tokio::test]
async fn address_reads_parse_each_answer() {
    let txid = block_1().transactions[0].hash();
    let mock = mainnet(1, move |method, _| match method {
        "getaddresstxids" => Reply::Result(json!([txid.to_string()])),
        "getaddressbalance" => Reply::Result(json!({"balance": 5, "received": 7})),
        _ => Reply::Result(
            serde_json::to_value(vec![Utxo::new(
                ADDRESS.parse().unwrap(),
                txid,
                transparent::OutputIndex::from_index(2),
                transparent::Script::new(&[0x51]),
                9,
                block::Height(1),
            )])
            .unwrap(),
        ),
    })
    .await;
    let backend = backend(&mock).await;
    let heights = block::Height(1)..=block::Height(5);
    assert_eq!(backend.address_transaction_ids(address(), heights).await.unwrap(), [txid]);
    assert_eq!(last_params(&mock), json!([{"addresses": [ADDRESS], "start": 1, "end": 5}]));
    let requests = mock.seen().len();
    let genesis = block::Height(0)..=block::Height(0);
    assert!(backend.address_transaction_ids(address(), genesis).await.unwrap().is_empty());
    assert_eq!(mock.seen().len(), requests);
    // The tip of the mock is 1, so a range from 2 has no transactions.
    let above = block::Height(2)..=block::Height(9);
    assert!(backend.address_transaction_ids(address(), above).await.unwrap().is_empty());
    assert_eq!(mock.seen().last().unwrap().body["method"], "getblockcount");

    let addresses = HashSet::from([address()]);
    assert_eq!(
        backend.address_balance(addresses.clone()).await.unwrap(),
        Amount::<NonNegative>::try_from(5_u64).unwrap()
    );
    assert_eq!(last_params(&mock), json!([{"addresses": [ADDRESS]}]));
    assert_eq!(
        backend.address_utxos(addresses).await.unwrap(),
        [AddressUtxo {
            address: address(),
            outpoint: transparent::OutPoint { hash: txid, index: 2 },
            output: transparent::Output {
                value: Amount::<NonNegative>::try_from(9_u64).unwrap(),
                lock_script: transparent::Script::new(&[0x51]),
            },
            height: block::Height(1),
        }]
    );
    assert_eq!(last_params(&mock), json!([{"addresses": [ADDRESS], "chainInfo": false}]));
}

#[tokio::test]
async fn mempool_caches_and_skips_what_is_not_pending() {
    let block = block_1();
    let mined: Block = BLOCK_MAINNET_2_BYTES.zcash_deserialize_into().unwrap();
    let (pending, mined) = (block.transactions[0].clone(), mined.transactions[0].clone());
    let (pending_id, mined_id) = (pending.hash(), mined.hash());
    let (gone, failing) = (transaction::Hash([1; 32]), transaction::Hash([2; 32]));
    let mock = mainnet(1, move |method, params| {
        let id = params[0].as_str().unwrap_or_default();
        match method {
            "getrawmempool" => {
                Reply::Result(json!([pending_id, mined_id, gone, failing].map(|id| id.to_string())))
            }
            _ if id == pending_id.to_string() => Reply::Result(transaction_object(&pending, None)),
            _ if id == mined_id.to_string() => {
                Reply::Result(transaction_object(&mined, Some(block::Height(2))))
            }
            _ if id == failing.to_string() => Reply::Close,
            _ => Reply::Error { code: RpcCode::INVALID_ADDRESS_OR_KEY, message: "not found" },
        }
    })
    .await;
    let backend = backend(&mock).await;
    let ids =
        |mempool: Vec<UnminedTx>| mempool.iter().map(|tx| tx.id().mined_id()).collect::<Vec<_>>();
    assert_eq!(ids(backend.mempool_transactions().await.unwrap()), [pending_id]);

    // The second read asks again only for the transactions that are not in the cache.
    let requests = mock.seen().len();
    assert_eq!(ids(backend.mempool_transactions().await.unwrap()), [pending_id]);
    let asked: HashSet<_> = mock.seen()[requests..]
        .iter()
        .filter(|seen| seen.body["method"] == "getrawtransaction")
        .map(|seen| seen.body["params"][0].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(asked, HashSet::from([mined_id, gone, failing].map(|id| id.to_string())));
}

#[tokio::test]
async fn mempool_cache_keeps_only_the_listed_ids() {
    let pending = block_1().transactions[0].clone();
    let id = pending.hash();
    let listed = Arc::new(AtomicBool::new(true));
    let in_mempool = listed.clone();
    let mock = mainnet(1, move |method, _| match method {
        "getrawmempool" if in_mempool.load(Ordering::SeqCst) => {
            Reply::Result(json!([id.to_string()]))
        }
        "getrawmempool" => Reply::Result(json!([])),
        _ => Reply::Result(transaction_object(&pending, None)),
    })
    .await;
    let backend = backend(&mock).await;
    assert_eq!(backend.mempool_transactions().await.unwrap().len(), 1);
    assert!(backend.lock_mempool().contains_key(&id));

    // The transaction left the mempool, so the cache drops it.
    listed.store(false, Ordering::SeqCst);
    assert!(backend.mempool_transactions().await.unwrap().is_empty());
    assert!(backend.lock_mempool().is_empty());
}

#[tokio::test]
async fn submit_maps_a_node_error_to_rejected() {
    let coinbase = block_1().transactions[0].clone();
    let id = coinbase.hash();
    let mock = mainnet(1, move |_, _| Reply::Result(json!(id.to_string()))).await;
    let accepting = backend(&mock).await;
    assert_eq!(accepting.submit_transaction((*coinbase).clone()).await.unwrap(), id);
    assert_eq!(
        last_params(&mock),
        json!([hex::encode(coinbase.zcash_serialize_to_vec().unwrap())])
    );

    let mock = mainnet(1, |_, _| Reply::Error { code: RpcCode(-26), message: "bad-txns" }).await;
    let error = backend(&mock).await.submit_transaction((*coinbase).clone()).await.unwrap_err();
    assert!(matches!(&error, BackendError::Rejected(message) if message.contains("bad-txns")));
    // A node in warm-up says nothing about the transaction.
    let mock =
        mainnet(1, |_, _| Reply::Error { code: RpcCode::IN_WARMUP, message: "warm-up" }).await;
    let error = backend(&mock).await.submit_transaction((*coinbase).clone()).await.unwrap_err();
    assert!(matches!(error, BackendError::Unavailable(_)), "{error}");
}
