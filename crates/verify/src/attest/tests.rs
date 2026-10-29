use bytes::Bytes;
use pasta_curves::pallas;
use serde_json::{Value, json};
use zakura_chain::{ironwood, orchard, sapling};
use ztreamer_chain::{
    codec::{RANGE_SIZE, TreeSizes, encode_range},
    test_utils::{hash, record},
};
use ztreamer_index::index::range_meta::Attested;
use ztreamer_remote::{
    RpcClient,
    test_utils::{MockNode, Reply},
};
use ztreamer_snapshot::{
    manifest_entry::{Frontiers, ManifestEntry},
    pool::SubtreeRoot,
};

use super::*;
use crate::{proof::EpochProof, range::Check};
use ztreamer_snapshot::{epoch::Epochs, test_utils::synthetic_entries};

const SUBTREE: NoteCommitmentSubtreeIndex = NoteCommitmentSubtreeIndex(5);

fn sapling_node(byte: u8) -> SubtreeNode {
    SubtreeNode::Sapling(sapling_crypto::Node::from_bytes([byte; 32]).unwrap())
}

/// The roots of an honest node: Sapling and Orchard are active, Ironwood is not active.
fn roots() -> PoolRoots {
    PoolRoots { ironwood: None, ..empty_tree_roots() }
}

/// The root of the empty tree of each pool.
fn empty_tree_roots() -> PoolRoots {
    PoolRoots {
        sapling: Some(sapling::tree::NoteCommitmentTree::default().root()),
        orchard: Some(orchard::tree::NoteCommitmentTree::default().root()),
        ironwood: Some(ironwood::tree::NoteCommitmentTree::default().root()),
    }
}

/// Answers from a table. A height or a subtree that is not in the table has no answer.
#[derive(Default)]
struct FakeAttestor {
    hashes: BTreeMap<u32, block::Hash>,
    roots: BTreeMap<u32, PoolRoots>,
    subtrees: BTreeMap<(Pool, NoteCommitmentSubtreeIndex), SubtreeNode>,
}

impl FakeAttestor {
    /// The honest answers for [`attest`].
    fn honest() -> Self {
        Self {
            hashes: [999, 1_999, 2_999].into_iter().map(|height| (height, hash(height))).collect(),
            roots: BTreeMap::from([(2_999, roots())]),
            subtrees: BTreeMap::from([((Pool::Sapling, SUBTREE), sapling_node(1))]),
        }
    }
}

fn no_answer() -> NodeError {
    NodeError::Rpc(RpcError::Config("no answer".to_owned()))
}

#[async_trait]
impl Attestor for FakeAttestor {
    async fn block_hash(&self, height: u32) -> Result<block::Hash, NodeError> {
        self.hashes.get(&height).copied().ok_or_else(no_answer)
    }

    async fn tree_roots(&self, height: u32) -> Result<PoolRoots, NodeError> {
        self.roots.get(&height).copied().ok_or_else(no_answer)
    }

    async fn subtree_roots(
        &self,
        pool: Pool,
        start: NoteCommitmentSubtreeIndex,
        count: u16,
    ) -> Result<Vec<SubtreeNode>, NodeError> {
        (start.0..start.0 + count)
            .map(|index| self.subtrees.get(&(pool, NoteCommitmentSubtreeIndex(index))).copied())
            .collect::<Option<_>>()
            .ok_or_else(no_answer)
    }
}

/// Attests one epoch of three ranges, 0 to 2,999, with `attestor`. Each range adds one
/// Sapling commitment. The second range completes Sapling subtree [`SUBTREE`]. The proof has
/// the root of the empty tree for each pool.
async fn attest(attestor: &FakeAttestor) -> Result<Vec<Attested>, EpochError> {
    let completed = SubtreeRoot { index: SUBTREE, root: sapling_node(1) };
    let mut entries = synthetic_entries(&[TreeSizes { sapling: 1, ..TreeSizes::default() }; 3]);
    entries[1].subtree_roots = vec![completed];
    let proof = EpochProof {
        end_frontiers: Frontiers::default(),
        end_tree_sizes: entries[2].end_tree_sizes,
        roots: empty_tree_roots(),
        completed: vec![completed],
    };
    proof.attest(&Epochs::new(&entries).next().unwrap(), attestor).await
}

fn assert_rejected(result: Result<Vec<Attested>, EpochError>, check: Check, height: u32) {
    match result {
        Err(EpochError::Reject(reject)) => {
            assert_eq!(reject, Reject::Rejected { check, height });
        }
        other => panic!("expected {check:?} at {height}, got {other:?}"),
    }
}

#[tokio::test]
async fn matching_answers_give_one_attested_record_per_range() {
    let attested = attest(&FakeAttestor::honest()).await.unwrap();

    // The first two ranges have the rebuilt roots of their frontiers. They have no Ironwood
    // root, because the node gives no Ironwood root at the end. Only the second range
    // completes the subtree.
    let record = |end_height, roots, subtree_roots| Attested {
        end_hash: hash(end_height),
        roots,
        subtree_roots,
    };
    let completed = vec![SubtreeRoot { index: SUBTREE, root: sapling_node(1) }];
    assert_eq!(
        attested,
        [
            record(999, roots(), Vec::new()),
            record(1_999, roots(), completed),
            record(2_999, roots(), Vec::new()),
        ]
    );
}

#[tokio::test]
async fn a_wrong_answer_rejects_with_its_check() {
    let mut attestor = FakeAttestor::honest();
    attestor.hashes.insert(2_999, hash(2_998));
    assert_rejected(attest(&attestor).await, Check::AttestedHash, 2_999);

    let mut attestor = FakeAttestor::honest();
    attestor.hashes.insert(1_999, hash(1_998));
    assert_rejected(attest(&attestor).await, Check::AttestedHash, 1_999);

    let mut orchard = orchard::tree::NoteCommitmentTree::default();
    orchard.append(pallas::Base::from(1)).unwrap();
    let mut attestor = FakeAttestor::honest();
    attestor.roots.insert(2_999, PoolRoots { orchard: Some(orchard.root()), ..roots() });
    assert_rejected(attest(&attestor).await, Check::AttestedRoots, 2_999);

    // The rebuilt Sapling tree has 3 commitments. Thus the node must give a Sapling root.
    let mut attestor = FakeAttestor::honest();
    attestor.roots.insert(2_999, PoolRoots { sapling: None, ..roots() });
    assert_rejected(attest(&attestor).await, Check::AttestedRoots, 2_999);

    let mut attestor = FakeAttestor::honest();
    attestor.subtrees.insert((Pool::Sapling, SUBTREE), sapling_node(2));
    assert_rejected(attest(&attestor).await, Check::AttestedSubtree, 1_999);
}

#[tokio::test]
async fn no_answer_is_a_node_error() {
    let mut attestor = FakeAttestor::honest();
    attestor.subtrees.clear();

    let result = attest(&attestor).await;

    assert!(matches!(result, Err(EpochError::Node { height: 2_999, .. })), "{result:?}");
}

/// The `z_gettreestate` answer at `height`, with the Sapling root of [`roots`] in reversed
/// byte order and the Orchard root in stored byte order, as the node prints them.
fn treestate(height: u32) -> Value {
    let roots = roots();
    let mut sapling = <[u8; 32]>::from(roots.sapling.unwrap());
    sapling.reverse();
    let orchard = <[u8; 32]>::from(roots.orchard.unwrap());
    json!({
        "hash": hash(height).to_string(),
        "height": height,
        "time": 0,
        "sapling": { "commitments": { "finalRoot": hex::encode(sapling) } },
        "orchard": { "commitments": { "finalRoot": hex::encode(orchard) } },
        "ironwood": { "commitments": {} },
    })
}

/// The `z_getsubtreesbyindex` answer for `start` and `limit`. The node has Sapling subtrees
/// 5 and 6, with the roots `sapling_node(1)` and `sapling_node(2)`.
fn subtrees(start: u64, limit: u64) -> Value {
    let subtrees: Vec<_> = (start..start + limit)
        .filter(|index| (5..=6).contains(index))
        .map(|index| json!({ "root": hex::encode([index as u8 - 4; 32]), "end_height": 1_999 }))
        .collect();
    json!({ "pool": "sapling", "start_index": start, "subtrees": subtrees })
}

/// A mock node that answers each method of the attestor, and the attestor on it.
async fn json_rpc_attestor(treestate: Value) -> (MockNode, JsonRpcAttestor) {
    let node = MockNode::start(move |_, seen| match seen.body["method"].as_str().unwrap() {
        "getblockhash" => {
            let height = seen.body["params"][0].as_u64().unwrap();
            Reply::Result(json!(hash(u32::try_from(height).unwrap()).to_string()))
        }
        "z_gettreestate" => Reply::Result(treestate.clone()),
        "z_getsubtreesbyindex" => {
            let params = &seen.body["params"];
            Reply::Result(subtrees(params[1].as_u64().unwrap(), params[2].as_u64().unwrap()))
        }
        method => panic!("unexpected method {method}"),
    })
    .await;
    let attestor = JsonRpcAttestor::new(ZakuraClient::new(RpcClient::new(node.config()).unwrap()));
    (node, attestor)
}

#[tokio::test]
async fn json_rpc_attestor_reads_each_pool_in_its_byte_order() {
    let (node, attestor) = json_rpc_attestor(treestate(2_999)).await;

    assert_eq!(attestor.tree_roots(2_999).await.unwrap(), roots());
    assert_eq!(attestor.block_hash(2_999).await.unwrap(), hash(2_999));
    assert_eq!(
        attestor.subtree_roots(Pool::Sapling, SUBTREE, 2).await.unwrap(),
        [sapling_node(1), sapling_node(2)]
    );

    let calls = node.seen();
    assert_eq!(calls[1].body["params"], json!([2_999]));
    assert_eq!(calls[2].body["params"], json!(["sapling", 5, 2]));
}

#[tokio::test]
async fn json_rpc_attestor_asks_the_node_once_for_the_tree_roots() {
    let (node, attestor) = json_rpc_attestor(treestate(2_999)).await;

    // Two calls at the same time, then a retry.
    let (first, second) = tokio::join!(attestor.tree_roots(2_999), attestor.tree_roots(2_999));
    assert_eq!((first.unwrap(), second.unwrap()), (roots(), roots()));
    attestor.tree_roots(2_999).await.unwrap();

    assert_eq!(node.seen().len(), 1);
}

#[tokio::test]
async fn json_rpc_attestor_refuses_a_tree_state_without_a_root() {
    let mut state = treestate(2_999);
    state["orchard"] = json!({ "commitments": { "finalState": "00" } });
    let (_node, attestor) = json_rpc_attestor(state).await;

    let error = attestor.tree_roots(2_999).await.unwrap_err();

    assert!(matches!(error, NodeError::MissingRoot { pool: Pool::Orchard }), "{error:?}");
}

#[tokio::test]
async fn json_rpc_attestor_rejects_an_answer_for_another_height() {
    let (node, attestor) = json_rpc_attestor(treestate(2_998)).await;

    let error = attestor.tree_roots(2_999).await.unwrap_err();

    assert!(matches!(error, NodeError::Height { expected: 2_999, actual: 2_998 }), "{error:?}");
    // The attestor keeps no failed answer: a retry asks the node again.
    assert!(attestor.lock_roots().is_empty());
    attestor.tree_roots(2_999).await.unwrap_err();
    assert_eq!(node.seen().len(), 2);
}

#[tokio::test]
async fn a_full_json_rpc_attestor_drops_the_lowest_filled_height() {
    let (_node, attestor) = json_rpc_attestor(treestate(2_999)).await;
    let filled = || Arc::new(OnceCell::new_with(Some(roots())));
    let heights = (0..CACHE_CAPACITY as u32).map(|at| 10_000 + at);
    attestor.lock_roots().extend(heights.map(|height| (height, filled())));
    // A call fills the cell of the lowest height. The map must not drop it.
    attestor.lock_roots().insert(9_000, Arc::new(OnceCell::new()));
    attestor.lock_roots().remove(&(10_000 + CACHE_CAPACITY as u32 - 1));

    attestor.tree_roots(2_999).await.unwrap();

    let map = attestor.lock_roots();
    assert_eq!(map.len(), CACHE_CAPACITY);
    assert!(map.contains_key(&2_999) && map.contains_key(&9_000));
    assert!(!map.contains_key(&10_000) && map.contains_key(&10_001));
}

#[tokio::test]
async fn json_rpc_attestor_names_a_missing_subtree() {
    let (_node, attestor) = json_rpc_attestor(treestate(2_999)).await;

    // The node has subtree 6, not subtree 7.
    let error =
        attestor.subtree_roots(Pool::Sapling, NoteCommitmentSubtreeIndex(6), 2).await.unwrap_err();

    assert!(
        matches!(
            error,
            NodeError::MissingSubtree { pool: Pool::Sapling, index: NoteCommitmentSubtreeIndex(7) }
        ),
        "{error:?}"
    );
}

/// The first mainnet epoch, 0 to 9,999, as the filler writes it. It has no commitments. Each
/// pool has the serialized empty frontier `000000`, also a pool that is not active.
fn first_epoch() -> (Vec<ManifestEntry>, Vec<Bytes>) {
    let empty = vec![0; 3];
    let blocks: Vec<_> = (0..10 * RANGE_SIZE).map(|height| record(height, Vec::new())).collect();
    blocks
        .chunks(RANGE_SIZE as usize)
        .map(|range| {
            let blob = encode_range(range).unwrap();
            let mut entry = ManifestEntry::from_records_and_blob(range, &blob);
            entry.frontiers = Frontiers {
                sapling: empty.clone(),
                orchard: empty.clone(),
                ironwood: empty.clone(),
            };
            (entry, Bytes::from(blob))
        })
        .unzip()
}

/// Verifies [`first_epoch`], and attests it with a mock node that gives `treestate` at 9,999.
async fn attest_first_epoch(treestate: Value) -> Result<Vec<Attested>, EpochError> {
    let (entries, blobs) = first_epoch();
    let epoch = Epochs::new(&entries).next().unwrap();
    assert_eq!(epoch.end_height(), 9_999);
    let proof = EpochProof::rebuild(&epoch, &blobs, &Frontiers::default()).unwrap();
    let (_node, attestor) = json_rpc_attestor(treestate).await;
    proof.attest(&epoch, &attestor).await
}

/// The `z_gettreestate` answer of zakura at 9,999 on mainnet. No pool is active.
fn inactive_treestate() -> Value {
    let mut state = treestate(9_999);
    for pool in ["sapling", "orchard", "ironwood"] {
        state[pool] = json!({ "commitments": {} });
    }
    state
}

#[tokio::test]
async fn an_epoch_with_empty_trees_verifies_when_the_node_gives_no_roots() {
    let attested = attest_first_epoch(inactive_treestate()).await.unwrap();

    assert_eq!(attested.len(), 10);
    assert!(attested.iter().all(|record| record.roots == PoolRoots::default()), "{attested:?}");
}

#[tokio::test]
async fn an_active_pool_with_an_empty_tree_verifies_with_the_empty_tree_root() {
    let mut state = inactive_treestate();
    let mut sapling = <[u8; 32]>::from(empty_tree_roots().sapling.unwrap());
    sapling.reverse();
    state["sapling"] =
        json!({ "commitments": { "finalRoot": hex::encode(sapling), "finalState": "000000" } });

    let attested = attest_first_epoch(state).await.unwrap();

    let sapling_only = PoolRoots { sapling: empty_tree_roots().sapling, ..PoolRoots::default() };
    assert!(attested.iter().all(|record| record.roots == sapling_only), "{attested:?}");
}

#[test]
fn a_fetch_error_keeps_its_cause_in_the_node_error() {
    let cancelled = NodeError::from(FetchAtHeightError::Cancelled(BlockingTaskCancelled));
    assert!(matches!(cancelled, NodeError::Cancelled(BlockingTaskCancelled)));
    let height = BlockAtHeightError::Height { expected: 3, actual: 4 };
    let height = NodeError::from(FetchAtHeightError::Block(height));
    assert!(matches!(height, NodeError::Height { expected: 3, actual: 4 }));
}
