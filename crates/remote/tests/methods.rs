//! Typed calls against captured node responses.
//!
//! The JSON in `vectors/` is from the serialization tests of zakura v1.5.1
//! (`crates/zakura-rpc/tests`). Those tests use mainnet responses. The raw block is mainnet block
//! 1 from `zakura_test`. The mock returns each response as the call result. Each test checks the
//! decoded value and the request that the client sent.

use serde_json::{Value, json};
use zakura_rpc::{
    client::{
        GetAddressBalanceRequest, GetAddressTxIdsRequest, GetAddressUtxosResponse,
        GetRawMempoolResponse, GetRawTransactionResponse, HexData,
        zakura_chain::{
            block::{self, Block},
            serialization::ZcashDeserializeInto as _,
            subtree::NoteCommitmentSubtreeIndex,
            transaction,
        },
    },
    methods::GetAddressUtxosRequest,
};
use zakura_state::HashOrHeight;
use zakura_test::vectors::BLOCK_MAINNET_1_BYTES;
use ztreamer_remote::{
    RpcClient, ZakuraClient,
    test_utils::{MockNode, Reply},
};

const BLOCK_HASH: &str = "00000000007bacdb373ca240dc6f044f0a816a407bc1924f82a2d84ebfa6103f";
const TXID: &str = "603df6640eade811df35190e544560531b9e4fbe3e13e423a191fa5598b2a0ea";
const ADDRESS: &str = "t1at7nVNsv6taLRrNRvnQdtfLNRDfsGc3Ak";

/// A client on a mock that returns `result` to all requests.
async fn node(result: Value) -> (MockNode, ZakuraClient) {
    let mock = MockNode::start(move |_, _| Reply::Result(result.clone())).await;
    let client = ZakuraClient::new(RpcClient::new(mock.config()).unwrap());
    (mock, client)
}

fn vector(json: &str) -> Value {
    serde_json::from_str(json).unwrap()
}

/// The method and params of the one request that the mock received.
fn sent(mock: &MockNode) -> (String, Value) {
    let seen = mock.seen();
    assert_eq!(seen.len(), 1);
    let body = &seen[0].body;
    (body["method"].as_str().unwrap().to_owned(), body["params"].clone())
}

#[tokio::test]
async fn get_blockchain_info() {
    let (mock, node) = node(vector(include_str!("vectors/getblockchaininfo.json"))).await;
    let info = node.get_blockchain_info().await.unwrap();
    assert_eq!(info.chain(), "main");
    assert_eq!(info.blocks(), block::Height(2932117));
    assert_eq!(
        info.best_block_hash().to_string(),
        "00000000005f47661fb9db53214c1206ddb784de28af47b79e6232ff850349e3"
    );
    assert_eq!(sent(&mock), ("getblockchaininfo".into(), json!([])));
}

#[tokio::test]
async fn get_block_count() {
    let (mock, node) = node(json!(2932117)).await;
    assert_eq!(node.get_block_count().await.unwrap(), block::Height(2932117));
    assert_eq!(sent(&mock), ("getblockcount".into(), json!([])));
}

#[tokio::test]
async fn get_block_hash() {
    let (mock, node) = node(json!(BLOCK_HASH)).await;
    let hash = node.get_block_hash(block::Height(2222000)).await.unwrap();
    assert_eq!(hash.hash().to_string(), BLOCK_HASH);
    assert_eq!(sent(&mock), ("getblockhash".into(), json!([2222000])));
}

#[tokio::test]
async fn get_block_raw() {
    let raw = serde_json::to_value(HexData(BLOCK_MAINNET_1_BYTES.clone())).unwrap();
    let (mock, node) = node(raw).await;
    let data = node.get_block_raw(HashOrHeight::Height(block::Height(1))).await.unwrap();
    assert_eq!(data.0, *BLOCK_MAINNET_1_BYTES);
    let block: Block = data.0.zcash_deserialize_into().unwrap();
    assert_eq!(block.coinbase_height(), Some(block::Height(1)));
    assert_eq!(sent(&mock), ("getblock".into(), json!(["1", 0])));
}

#[tokio::test]
async fn z_get_treestate() {
    let (mock, node) = node(vector(include_str!("vectors/z_gettreestate.json"))).await;
    let treestate =
        node.z_get_treestate(HashOrHeight::Height(block::Height(2931720))).await.unwrap();
    assert_eq!(
        treestate.hash().to_string(),
        "000000000154f210e2451c45a192c69d12c0db18a427be13be3913e0feecd6f6"
    );
    assert_eq!(treestate.height(), block::Height(2931720));
    assert_eq!(sent(&mock), ("z_gettreestate".into(), json!(["2931720"])));
}

#[tokio::test]
async fn z_get_subtrees_by_index() {
    let response = vector(include_str!("vectors/z_getsubtreesbyindex.json"));
    let (mock, node) = node(response).await;
    let subtrees = node
        .z_get_subtrees_by_index(
            "orchard",
            NoteCommitmentSubtreeIndex(0),
            Some(NoteCommitmentSubtreeIndex(1)),
        )
        .await
        .unwrap();
    assert_eq!(subtrees.pool(), "orchard");
    assert_eq!(subtrees.subtrees().len(), 1);
    assert_eq!(subtrees.subtrees()[0].end_height, block::Height(1707429));
    assert_eq!(sent(&mock), ("z_getsubtreesbyindex".into(), json!(["orchard", 0, 1])));
}

#[tokio::test]
async fn z_get_subtrees_by_index_without_limit() {
    let (mock, node) = node(vector(include_str!("vectors/z_getsubtreesbyindex.json"))).await;
    node.z_get_subtrees_by_index("orchard", NoteCommitmentSubtreeIndex(0), None).await.unwrap();
    assert_eq!(sent(&mock), ("z_getsubtreesbyindex".into(), json!(["orchard", 0])));
}

#[tokio::test]
async fn get_raw_mempool() {
    let (mock, node) = node(vector(include_str!("vectors/getrawmempool.json"))).await;
    let GetRawMempoolResponse::TxIds(txids) = node.get_raw_mempool().await.unwrap() else {
        panic!("expected transaction ids");
    };
    assert_eq!(txids, ["77ec13dde45185e99dba408d592c5b30438e8c71af5b6e2d9f4d29cb4da8ccbf"]);
    assert_eq!(sent(&mock), ("getrawmempool".into(), json!([false])));
}

#[tokio::test]
async fn get_raw_transaction_verbose() {
    let response = vector(include_str!("vectors/getrawtransaction_verbose.json"));
    let (mock, node) = node(response).await;
    let txid: transaction::Hash = TXID.parse().unwrap();
    let GetRawTransactionResponse::Object(transaction) =
        node.get_raw_transaction(txid).await.unwrap()
    else {
        panic!("expected a transaction object");
    };
    assert_eq!(transaction.height(), Some(2222000));
    assert_eq!(sent(&mock), ("getrawtransaction".into(), json!([TXID, 1])));
}

#[tokio::test]
async fn send_raw_transaction() {
    let (mock, node) = node(json!(TXID)).await;
    let sent_txid = node.send_raw_transaction(&[0xde, 0xad, 0xbe, 0xef]).await.unwrap();
    assert_eq!(sent_txid.hash().to_string(), TXID);
    assert_eq!(sent(&mock), ("sendrawtransaction".into(), json!(["deadbeef"])));
}

#[tokio::test]
async fn get_address_tx_ids() {
    let (mock, node) = node(json!([TXID])).await;
    let request =
        GetAddressTxIdsRequest::new(vec![ADDRESS.to_owned()], Some(2931856), Some(2932856));
    let txids = node.get_address_tx_ids(&request).await.unwrap();
    assert_eq!(txids, [TXID]);
    assert_eq!(
        sent(&mock),
        (
            "getaddresstxids".into(),
            json!([{"addresses": [ADDRESS], "start": 2931856, "end": 2932856}])
        )
    );
}

#[tokio::test]
async fn get_address_balance() {
    let (mock, node) = node(vector(include_str!("vectors/getaddressbalance.json"))).await;
    let request = GetAddressBalanceRequest::new(vec![ADDRESS.to_owned()]);
    let balance = node.get_address_balance(&request).await.unwrap();
    assert_eq!(balance.balance(), 11290259389);
    assert_eq!(balance.received, 11290259390);
    assert_eq!(sent(&mock), ("getaddressbalance".into(), json!([{"addresses": [ADDRESS]}])));
}

#[tokio::test]
async fn get_address_utxos() {
    let (mock, node) = node(vector(include_str!("vectors/getaddressutxos.json"))).await;
    let request = GetAddressUtxosRequest::new(vec![ADDRESS.to_owned()], false);
    let GetAddressUtxosResponse::Utxos(utxos) = node.get_address_utxos(&request).await.unwrap()
    else {
        panic!("expected a list of outputs");
    };
    assert_eq!(utxos.len(), 1);
    assert_eq!(utxos[0].satoshis(), 125000000);
    assert_eq!(utxos[0].height(), block::Height(2931856));
    assert_eq!(
        sent(&mock),
        ("getaddressutxos".into(), json!([{"addresses": [ADDRESS], "chainInfo": false}]))
    );
}
