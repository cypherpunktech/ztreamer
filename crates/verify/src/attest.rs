//! Attestation: the node confirms what an epoch rebuilt.
//!
//! [`Epoch::verify`](crate::epoch::Epoch::verify) proves that the ranges of an epoch agree with
//! the manifest. [`Epoch::attest`] then asks the node about the epoch: the block hash at the end
//! of each range, the three tree roots at the end of the epoch, once, and the roots of the
//! subtrees that the epoch completed, with one call per pool. When all answers agree, it returns
//! one [`Attested`] record per range, which the importer keeps in `range_meta`.
//!
//! [`JsonRpcAttestor`] asks a zakura node over JSON-RPC. `z_gettreestate` takes 0.2 to 2.6 s and
//! the node does not cache it, so the attestor keeps the answers that it gave: a retry of an
//! epoch makes no new call.

use std::{
    collections::BTreeMap,
    sync::{Mutex, MutexGuard, PoisonError},
};

use async_trait::async_trait;
use zakura_chain::{block, subtree::NoteCommitmentSubtreeIndex};
use zakura_rpc::client::Commitments;
use zakura_state::HashOrHeight;
use ztreamer_indexer::{
    codec::CodecError,
    frontier::{FromRpc, RpcDecodeError},
    index::range_meta::{Attested, PoolRoots, SubtreeNode},
    parser::CompactParseError,
    types::RawBlockError,
};
use ztreamer_remote::{RpcError, ZakuraClient};

use crate::{
    epoch::{Epoch, EpochProof, Trees},
    manifest::Pool,
    range::{Check, Reject},
};

/// The most answers of each kind that a [`JsonRpcAttestor`] keeps.
pub const CACHE_CAPACITY: usize = 1024;

/// Why a node gave no answer.
#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    /// The call to the node failed.
    #[error(transparent)]
    Rpc(#[from] RpcError),
    /// The node answered for another height.
    #[error("node answered for height {actual}, not {expected}")]
    Height {
        /// The height that the call asked for.
        expected: u32,
        /// The height in the answer.
        actual: u32,
    },
    /// A `finalRoot` of `z_gettreestate` does not parse.
    #[error("{pool:?} final root does not parse")]
    Root {
        /// The pool of the root.
        pool: Pool,
        /// Why the root does not parse.
        #[source]
        source: RpcDecodeError,
    },
    /// `z_gettreestate` gives a `finalState` but no `finalRoot` for the pool. The pool is
    /// active, thus the answer has no root to compare.
    #[error("{pool:?} tree state has no final root")]
    MissingRoot {
        /// The pool of the tree state.
        pool: Pool,
    },
    /// The node does not have the subtree.
    #[error("node has no {pool:?} subtree {}", .index.0)]
    MissingSubtree {
        /// The pool of the subtree.
        pool: Pool,
        /// The index of the subtree.
        index: NoteCommitmentSubtreeIndex,
    },
    /// The subtree root is not the hex of a node of the pool.
    #[error("{pool:?} subtree root {} does not parse", .index.0)]
    SubtreeRoot {
        /// The pool of the subtree.
        pool: Pool,
        /// The index of the subtree.
        index: NoteCommitmentSubtreeIndex,
    },
    /// The node block does not deserialize, or has no coinbase height.
    #[error(transparent)]
    RawBlock(#[from] RawBlockError),
    /// The indexer parser does not read the node block.
    #[error(transparent)]
    Parse(#[from] CompactParseError),
    /// The record of the node block does not encode.
    #[error("record of the node block does not encode")]
    Encode(#[from] CodecError),
}

/// Why [`Epoch::attest`] or [`Epoch::sample`](crate::epoch::Epoch::sample) did not accept an
/// epoch.
#[derive(Debug, thiserror::Error)]
pub enum EpochError {
    /// A range of the epoch is wrong, or a blob must be fetched again.
    #[error(transparent)]
    Reject(#[from] Reject),
    /// The node gave no answer. This says nothing about the ranges: try again later.
    #[error("node gave no answer for height {height}")]
    Node {
        /// The height that the failed call asked about.
        height: u32,
        /// Why the node gave no answer.
        #[source]
        source: NodeError,
    },
}

/// A trusted source of the chain state at a height, usually a node.
///
/// # Implementation
///
/// Give the answers of the best chain. Return a [`NodeError`] when there is no answer, not a
/// default value: [`Epoch::attest`] rejects a range on an answer that does not agree. A root is
/// `None` when the pool is not active at the height. An implementation may cache its answers,
/// because the answers at a height below the tip do not change.
#[async_trait]
pub trait Attestor: Send + Sync {
    /// The hash of the best-chain block at `height`.
    async fn block_hash(&self, height: u32) -> Result<block::Hash, NodeError>;

    /// The three note commitment tree roots after the block at `height`.
    async fn tree_roots(&self, height: u32) -> Result<PoolRoots, NodeError>;

    /// The roots of the `count` completed subtrees of `pool` from `start`, in index order.
    /// Give exactly `count` roots, or [`NodeError::MissingSubtree`] for the first that the
    /// source does not have.
    async fn subtree_roots(
        &self,
        pool: Pool,
        start: NoteCommitmentSubtreeIndex,
        count: u16,
    ) -> Result<Vec<SubtreeNode>, NodeError>;
}

impl Epoch<'_> {
    /// Compares `proof`, the result of [`Epoch::verify`], with the answers of `attestor`, and
    /// returns one [`Attested`] record per entry, in order.
    ///
    /// The method compares the end hash of each entry with the node. It compares the three
    /// roots of `proof` at [`Epoch::end_height`] with [`EpochProof::matches_roots`]. It compares
    /// the root of each subtree that the epoch completed, with one [`Attestor::subtree_roots`]
    /// call per pool. A wrong subtree root rejects at the end height of the entry that lists it.
    ///
    /// The record of an entry holds the end hash of the node, the subtrees that the entry
    /// completed, and the roots at its end. The last entry has the roots of the node. An entry
    /// before it has the rebuilt roots of its frontiers: the node attests them through the end
    /// roots, because the rebuild appends the leaves of the epoch to these trees. If the node
    /// gives no root for a pool at the end, the pool is not active before the end. Then the root
    /// of that pool is `None` in each record. In the epoch where a pool becomes active, an entry
    /// before the activation height has the root of the empty tree, not `None`. The frontier of
    /// the entry cannot show the difference.
    ///
    /// ```
    /// use ztreamer_indexer::index::range_meta::{Attested, Frontiers};
    /// use ztreamer_verify::{
    ///     attest::{Attestor, EpochError},
    ///     epoch::Epoch,
    /// };
    ///
    /// # async fn run(
    /// #     epoch: Epoch<'_>,
    /// #     blobs: Vec<Vec<u8>>,
    /// #     start: Frontiers,
    /// #     node: &dyn Attestor,
    /// # ) -> Result<Vec<Attested>, EpochError> {
    /// let proof = epoch.verify(&blobs, &start)?;
    /// epoch.attest(&proof, node).await
    /// # }
    /// ```
    pub async fn attest(
        &self,
        proof: &EpochProof,
        attestor: &dyn Attestor,
    ) -> Result<Vec<Attested>, EpochError> {
        for entry in self.entries() {
            let height = entry.end_height;
            let hash = attestor
                .block_hash(height)
                .await
                .map_err(|source| EpochError::Node { height, source })?;
            if hash != entry.end_hash {
                return Err(Check::AttestedHash.at(height).into());
            }
        }
        let end = self.end_height();
        let roots = attestor
            .tree_roots(end)
            .await
            .map_err(|source| EpochError::Node { height: end, source })?;
        if !proof.matches_roots(&roots) {
            return Err(Check::AttestedRoots.at(end).into());
        }
        self.attest_subtrees(proof, attestor).await?;
        self.entries()
            .iter()
            .map(|entry| {
                let roots = if entry.end_height == end {
                    roots
                } else {
                    // `Epoch::verify` checked that the frontiers are the rebuilt trees, thus
                    // they parse.
                    let rebuilt = Trees::try_from(&entry.frontiers)
                        .map_err(|_| Check::Frontier.at(entry.end_height))?
                        .roots();
                    PoolRoots {
                        sapling: roots.sapling.and(rebuilt.sapling),
                        orchard: roots.orchard.and(rebuilt.orchard),
                        ironwood: roots.ironwood.and(rebuilt.ironwood),
                    }
                };
                let subtree_roots = entry.subtree_roots.clone();
                Ok(Attested { end_hash: entry.end_hash, roots, subtree_roots })
            })
            .collect()
    }

    /// Compares each subtree root of `proof` with the node, with one call per pool.
    async fn attest_subtrees(
        &self,
        proof: &EpochProof,
        attestor: &dyn Attestor,
    ) -> Result<(), EpochError> {
        let end = self.end_height();
        for pool in Pool::ALL {
            let completed: Vec<_> =
                proof.completed.iter().filter(|root| Pool::from(&root.root) == pool).collect();
            let indices = completed.iter().map(|root| root.index.0);
            let (Some(start), Some(last)) = (indices.clone().min(), indices.max()) else {
                continue;
            };
            let answers = attestor
                .subtree_roots(
                    pool,
                    NoteCommitmentSubtreeIndex(start),
                    (last - start).saturating_add(1),
                )
                .await
                .map_err(|source| EpochError::Node { height: end, source })?;
            let wrong = completed.into_iter().find(|completed| {
                answers.get(usize::from(completed.index.0 - start)) != Some(&completed.root)
            });
            if let Some(wrong) = wrong {
                let entry = self.entries().iter().find(|entry| entry.subtree_roots.contains(wrong));
                let height = entry.map_or(end, |entry| entry.end_height);
                return Err(Check::AttestedSubtree.at(height).into());
            }
        }
        Ok(())
    }
}

/// An [`Attestor`] that asks a zakura node over JSON-RPC, and keeps the answers.
///
/// It keeps up to [`CACHE_CAPACITY`] answers of each kind. When a map is full, it drops the
/// answer for the lowest height or subtree index.
///
/// ```no_run
/// use ztreamer_remote::{RpcClient, ZakuraClient};
/// use ztreamer_verify::attest::{Attestor, JsonRpcAttestor};
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let config = ZakuraClient::config("http://127.0.0.1:8232".parse()?);
/// let attestor = JsonRpcAttestor::new(ZakuraClient::new(RpcClient::new(config)?));
/// let roots = attestor.tree_roots(3_524_999).await?;
/// # let _ = roots;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct JsonRpcAttestor {
    node: ZakuraClient,
    hashes: Answers<u32, block::Hash>,
    roots: Answers<u32, PoolRoots>,
    subtrees: Answers<(Pool, NoteCommitmentSubtreeIndex), SubtreeNode>,
}

impl JsonRpcAttestor {
    /// An attestor with no answers yet.
    pub const fn new(node: ZakuraClient) -> Self {
        Self { node, hashes: Answers::new(), roots: Answers::new(), subtrees: Answers::new() }
    }
}

#[async_trait]
impl Attestor for JsonRpcAttestor {
    async fn block_hash(&self, height: u32) -> Result<block::Hash, NodeError> {
        self.hashes
            .get_or_ask(height, || async {
                Ok(self.node.get_block_hash(block::Height(height)).await?.hash())
            })
            .await
    }

    async fn tree_roots(&self, height: u32) -> Result<PoolRoots, NodeError> {
        self.roots
            .get_or_ask(height, || async {
                let state =
                    self.node.z_get_treestate(HashOrHeight::Height(block::Height(height))).await?;
                let actual = state.height().0;
                if actual != height {
                    return Err(NodeError::Height { expected: height, actual });
                }
                Ok(PoolRoots {
                    sapling: final_root(Pool::Sapling, state.sapling().commitments())?,
                    orchard: final_root(Pool::Orchard, state.orchard().commitments())?,
                    ironwood: final_root(Pool::Ironwood, state.ironwood().commitments())?,
                })
            })
            .await
    }

    async fn subtree_roots(
        &self,
        pool: Pool,
        start: NoteCommitmentSubtreeIndex,
        count: u16,
    ) -> Result<Vec<SubtreeNode>, NodeError> {
        let keys = (0..count)
            .map(|offset| {
                start.0.checked_add(offset).map(|index| (pool, NoteCommitmentSubtreeIndex(index)))
            })
            .collect::<Option<Vec<_>>>()
            .ok_or(NodeError::MissingSubtree { pool, index: start })?;
        if let Some(known) = self.subtrees.known(&keys) {
            return Ok(known);
        }
        let limit = Some(NoteCommitmentSubtreeIndex(count));
        let response = self.node.z_get_subtrees_by_index(pool.as_str(), start, limit).await?;
        if response.start_index() != start {
            return Err(NodeError::MissingSubtree { pool, index: start });
        }
        let roots = keys
            .iter()
            .enumerate()
            .map(|(offset, &(_, index))| {
                let subtree = response
                    .subtrees()
                    .get(offset)
                    .ok_or(NodeError::MissingSubtree { pool, index })?;
                hex::decode(&subtree.root)
                    .ok()
                    .and_then(|bytes| bytes.try_into().ok())
                    .and_then(|bytes| pool.subtree_node(bytes))
                    .ok_or(NodeError::SubtreeRoot { pool, index })
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.subtrees.keep(keys.into_iter().zip(roots.iter().copied()));
        Ok(roots)
    }
}

/// Reads the `finalRoot` of `pool` in the byte order of the pool. See
/// [`ztreamer_indexer::frontier`]. A pool with no `finalState` is not active, and has no root.
/// It is a free function because it is generic over the root type of the pool.
fn final_root<R: FromRpc>(pool: Pool, commitments: &Commitments) -> Result<Option<R>, NodeError> {
    match (commitments.final_root(), commitments.final_state()) {
        (Some(root), _) => {
            R::from_rpc_bytes(root).map(Some).map_err(|source| NodeError::Root { pool, source })
        }
        (None, Some(_)) => Err(NodeError::MissingRoot { pool }),
        (None, None) => Ok(None),
    }
}

/// Answers that a node gave, by key. The map drops its lowest key when it is full.
#[derive(Debug)]
struct Answers<K, V>(Mutex<BTreeMap<K, V>>);

impl<K: Ord + Copy, V: Copy> Answers<K, V> {
    const fn new() -> Self {
        Self(Mutex::new(BTreeMap::new()))
    }

    /// The map. The map is valid after a panic, because each change is one call.
    fn map(&self) -> MutexGuard<'_, BTreeMap<K, V>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The answers for all of `keys`, in order, or `None` if the map does not have one of them.
    fn known(&self, keys: &[K]) -> Option<Vec<V>> {
        let map = self.map();
        keys.iter().map(|key| map.get(key).copied()).collect()
    }

    /// Keeps `answers`, and drops the lowest keys above [`CACHE_CAPACITY`].
    fn keep(&self, answers: impl IntoIterator<Item = (K, V)>) {
        let mut map = self.map();
        map.extend(answers);
        while map.len() > CACHE_CAPACITY {
            map.pop_first();
        }
    }

    /// The answer for `key`. If the map does not have it, `ask` gives it and the map keeps it.
    async fn get_or_ask<F>(&self, key: K, ask: impl FnOnce() -> F) -> Result<V, NodeError>
    where
        F: Future<Output = Result<V, NodeError>>,
    {
        if let Some(&[value]) = self.known(&[key]).as_deref() {
            return Ok(value);
        }
        let value = ask().await?;
        self.keep([(key, value)]);
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use pasta_curves::pallas;
    use serde_json::{Value, json};
    use zakura_chain::{ironwood, orchard, sapling};
    use ztreamer_indexer::{
        codec::{TreeSizes, encode_range},
        index::{
            RANGE_SIZE,
            range_meta::{Frontiers, ManifestEntry, SubtreeRoot},
        },
        test_utils::{hash, record},
    };
    use ztreamer_remote::{
        RpcClient,
        test_utils::{MockNode, Reply},
    };

    use super::*;
    use crate::{
        epoch::{Epochs, synthetic_entries},
        range::entry_for,
    };

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
                hashes: [999, 1_999, 2_999]
                    .into_iter()
                    .map(|height| (height, hash(height)))
                    .collect(),
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
        Epochs::new(&entries).next().unwrap().attest(&proof, attestor).await
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
        let attestor =
            JsonRpcAttestor::new(ZakuraClient::new(RpcClient::new(node.config()).unwrap()));
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
    async fn json_rpc_attestor_asks_the_node_once() {
        let (node, attestor) = json_rpc_attestor(treestate(2_999)).await;

        for _ in 0..2 {
            attestor.tree_roots(2_999).await.unwrap();
            attestor.block_hash(2_999).await.unwrap();
            attestor.subtree_roots(Pool::Sapling, SUBTREE, 2).await.unwrap();
        }
        // The answers of the batch also answer a part of it.
        let six = NoteCommitmentSubtreeIndex(6);
        assert_eq!(attestor.subtree_roots(Pool::Sapling, six, 1).await.unwrap(), [sapling_node(2)]);

        assert_eq!(node.seen().len(), 3);
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
        let (_node, attestor) = json_rpc_attestor(treestate(2_998)).await;

        let error = attestor.tree_roots(2_999).await.unwrap_err();

        assert!(matches!(error, NodeError::Height { expected: 2_999, actual: 2_998 }), "{error:?}");
    }

    #[tokio::test]
    async fn json_rpc_attestor_names_a_missing_subtree() {
        let (_node, attestor) = json_rpc_attestor(treestate(2_999)).await;

        // The node has subtree 6, not subtree 7.
        let error = attestor
            .subtree_roots(Pool::Sapling, NoteCommitmentSubtreeIndex(6), 2)
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                NodeError::MissingSubtree {
                    pool: Pool::Sapling,
                    index: NoteCommitmentSubtreeIndex(7)
                }
            ),
            "{error:?}"
        );
    }

    /// The first mainnet epoch, 0 to 9,999, as the filler writes it. It has no commitments. Each
    /// pool has the serialized empty frontier `000000`, also a pool that is not active.
    fn first_epoch() -> (Vec<ManifestEntry>, Vec<Vec<u8>>) {
        let empty = vec![0; 3];
        let blocks: Vec<_> =
            (0..10 * RANGE_SIZE).map(|height| record(height, Vec::new())).collect();
        blocks
            .chunks(RANGE_SIZE as usize)
            .map(|range| {
                let blob = encode_range(range).unwrap();
                let mut entry = entry_for(range, &blob);
                entry.frontiers = Frontiers {
                    sapling: empty.clone(),
                    orchard: empty.clone(),
                    ironwood: empty.clone(),
                };
                (entry, blob)
            })
            .unzip()
    }

    /// Verifies [`first_epoch`], and attests it with a mock node that gives `treestate` at 9,999.
    async fn attest_first_epoch(treestate: Value) -> Result<Vec<Attested>, EpochError> {
        let (entries, blobs) = first_epoch();
        let epoch = Epochs::new(&entries).next().unwrap();
        assert_eq!(epoch.end_height(), 9_999);
        let proof = epoch.verify(&blobs, &Frontiers::default()).unwrap();
        let (_node, attestor) = json_rpc_attestor(treestate).await;
        epoch.attest(&proof, &attestor).await
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

        let sapling_only =
            PoolRoots { sapling: empty_tree_roots().sapling, ..PoolRoots::default() };
        assert!(attested.iter().all(|record| record.roots == sapling_only), "{attested:?}");
    }
}
