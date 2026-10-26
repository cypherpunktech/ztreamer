//! Attestation: the node confirms what an epoch rebuilt.
//!
//! [`Epoch::verify`](crate::epoch::Epoch::verify) proves that the ranges of an epoch agree with
//! the manifest. [`Epoch::attest`] then asks the node about the end of the epoch, once: the block
//! hash, the three tree roots, and the root of each subtree that the epoch completed. When all
//! answers agree, it returns the [`Attested`] record that the importer keeps in `range_meta`.
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
use zakura_state::HashOrHeight;
use ztreamer_indexer::{
    frontier::{FromRpc, RpcDecodeError},
    index::range_meta::{Attested, PoolRoots, SubtreeNode},
};
use ztreamer_remote::{RpcError, ZakuraClient};

use crate::{
    epoch::{Epoch, EpochProof},
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
}

/// Why [`Epoch::attest`] did not attest an epoch.
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
/// Give the answers of the best chain. Return an [`NodeError`] when there is no answer, not a
/// default value: [`Epoch::attest`] rejects a range on an answer that does not agree. A root is
/// `None` when the pool is not active at the height. An implementation may cache its answers,
/// because the answers at a height below the tip do not change.
#[async_trait]
pub trait Attestor: Send + Sync {
    /// The hash of the best-chain block at `height`.
    async fn block_hash(&self, height: u32) -> Result<block::Hash, NodeError>;

    /// The three note commitment tree roots after the block at `height`.
    async fn tree_roots(&self, height: u32) -> Result<PoolRoots, NodeError>;

    /// The root of the completed subtree `index` of `pool`.
    async fn subtree_root(
        &self,
        pool: Pool,
        index: NoteCommitmentSubtreeIndex,
    ) -> Result<SubtreeNode, NodeError>;
}

impl Epoch<'_> {
    /// Compares `proof`, the result of [`Epoch::verify`], with the answers of `attestor` at the
    /// end of the epoch.
    ///
    /// The method compares [`Epoch::end_hash`], the three roots of `proof`, and each subtree
    /// root that the epoch completed. A wrong subtree root rejects at the end height of the entry
    /// that lists it. It returns the answers as an [`Attested`] record for the last entry of the
    /// epoch.
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
    /// # ) -> Result<Attested, EpochError> {
    /// let proof = epoch.verify(&blobs, &start)?;
    /// epoch.attest(&proof, node).await
    /// # }
    /// ```
    pub async fn attest(
        &self,
        proof: &EpochProof,
        attestor: &dyn Attestor,
    ) -> Result<Attested, EpochError> {
        let height = self.end_height();
        let node = |source| EpochError::Node { height, source };

        let end_hash = attestor.block_hash(height).await.map_err(node)?;
        if end_hash != self.end_hash() {
            return Err(Check::AttestedHash.at(height).into());
        }
        let roots = attestor.tree_roots(height).await.map_err(node)?;
        if roots != proof.roots {
            return Err(Check::AttestedRoots.at(height).into());
        }
        for completed in &proof.completed {
            let root = attestor
                .subtree_root(Pool::from(&completed.root), completed.index)
                .await
                .map_err(node)?;
            if root != completed.root {
                let entry =
                    self.entries().iter().find(|entry| entry.subtree_roots.contains(completed));
                let height = entry.map_or(height, |entry| entry.end_height);
                return Err(Check::AttestedSubtree.at(height).into());
            }
        }
        Ok(Attested { end_hash, roots, subtree_roots: proof.completed.clone() })
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
                    sapling: final_root(
                        Pool::Sapling,
                        state.sapling().commitments().final_root().as_deref(),
                    )?,
                    orchard: final_root(
                        Pool::Orchard,
                        state.orchard().commitments().final_root().as_deref(),
                    )?,
                    ironwood: final_root(
                        Pool::Ironwood,
                        state.ironwood().commitments().final_root().as_deref(),
                    )?,
                })
            })
            .await
    }

    async fn subtree_root(
        &self,
        pool: Pool,
        index: NoteCommitmentSubtreeIndex,
    ) -> Result<SubtreeNode, NodeError> {
        self.subtrees
            .get_or_ask((pool, index), || async {
                let one = Some(NoteCommitmentSubtreeIndex(1));
                let response = self.node.z_get_subtrees_by_index(pool.as_str(), index, one).await?;
                let subtree = response
                    .subtrees()
                    .first()
                    .filter(|_| response.start_index() == index)
                    .ok_or(NodeError::MissingSubtree { pool, index })?;
                hex::decode(&subtree.root)
                    .ok()
                    .and_then(|bytes| bytes.try_into().ok())
                    .and_then(|bytes| pool.subtree_node(bytes))
                    .ok_or(NodeError::SubtreeRoot { pool, index })
            })
            .await
    }
}

/// Reads a `finalRoot` in the byte order of `pool`. See [`ztreamer_indexer::frontier`].
fn final_root<R: FromRpc>(pool: Pool, root: Option<&[u8]>) -> Result<Option<R>, NodeError> {
    root.map(R::from_rpc_bytes).transpose().map_err(|source| NodeError::Root { pool, source })
}

/// Answers that a node gave, by key. The map drops its lowest key when it is full.
#[derive(Debug)]
struct Answers<K, V>(Mutex<BTreeMap<K, V>>);

impl<K: Ord, V: Copy> Answers<K, V> {
    const fn new() -> Self {
        Self(Mutex::new(BTreeMap::new()))
    }

    /// The map. The map is valid after a panic, because each change is one call.
    fn map(&self) -> MutexGuard<'_, BTreeMap<K, V>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The answer for `key`. If the map does not have it, `ask` gives it and the map keeps it.
    async fn get_or_ask<F>(&self, key: K, ask: impl FnOnce() -> F) -> Result<V, NodeError>
    where
        F: Future<Output = Result<V, NodeError>>,
    {
        let known = self.map().get(&key).copied();
        if let Some(value) = known {
            return Ok(value);
        }
        let value = ask().await?;
        let mut map = self.map();
        map.insert(key, value);
        if map.len() > CACHE_CAPACITY {
            map.pop_first();
        }
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};
    use zakura_chain::{orchard, sapling};
    use ztreamer_indexer::{
        codec::TreeSizes,
        index::range_meta::{Frontiers, SubtreeRoot},
        test_utils::hash,
    };
    use ztreamer_remote::{
        RpcClient,
        test_utils::{MockNode, Reply},
    };

    use super::*;
    use crate::epoch::{Epochs, synthetic_entries};

    const SUBTREE: NoteCommitmentSubtreeIndex = NoteCommitmentSubtreeIndex(5);

    fn sapling_node(byte: u8) -> SubtreeNode {
        SubtreeNode::Sapling(sapling_crypto::Node::from_bytes([byte; 32]).unwrap())
    }

    fn roots() -> PoolRoots {
        PoolRoots {
            sapling: Some(sapling::tree::NoteCommitmentTree::default().root()),
            orchard: Some(orchard::tree::NoteCommitmentTree::default().root()),
            ironwood: None,
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
                hashes: BTreeMap::from([(2_999, hash(2_999))]),
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

        async fn subtree_root(
            &self,
            pool: Pool,
            index: NoteCommitmentSubtreeIndex,
        ) -> Result<SubtreeNode, NodeError> {
            self.subtrees.get(&(pool, index)).copied().ok_or_else(no_answer)
        }
    }

    /// Attests one epoch of three ranges, 0 to 2,999, with `attestor`. The second range
    /// completes Sapling subtree [`SUBTREE`].
    async fn attest(attestor: &FakeAttestor) -> Result<Attested, EpochError> {
        let completed = SubtreeRoot { index: SUBTREE, root: sapling_node(1) };
        let mut entries = synthetic_entries(&[TreeSizes { sapling: 1, ..TreeSizes::default() }; 3]);
        entries[1].subtree_roots = vec![completed];
        let proof = EpochProof {
            end_frontiers: Frontiers::default(),
            roots: roots(),
            completed: vec![completed],
        };
        Epochs::new(&entries).next().unwrap().attest(&proof, attestor).await
    }

    fn assert_rejected(result: Result<Attested, EpochError>, check: Check, height: u32) {
        match result {
            Err(EpochError::Reject(reject)) => {
                assert_eq!(reject, Reject::Rejected { check, height });
            }
            other => panic!("expected {check:?} at {height}, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn matching_answers_give_the_attested_record() {
        let attested = attest(&FakeAttestor::honest()).await.unwrap();

        assert_eq!(
            attested,
            Attested {
                end_hash: hash(2_999),
                roots: roots(),
                subtree_roots: vec![SubtreeRoot { index: SUBTREE, root: sapling_node(1) }],
            }
        );
    }

    #[tokio::test]
    async fn a_wrong_answer_rejects_with_its_check() {
        let mut attestor = FakeAttestor::honest();
        attestor.hashes.insert(2_999, hash(2_998));
        assert_rejected(attest(&attestor).await, Check::AttestedHash, 2_999);

        let mut attestor = FakeAttestor::honest();
        attestor.roots.insert(2_999, PoolRoots { orchard: None, ..roots() });
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

    /// A mock node that answers each method of the attestor, and the attestor on it.
    async fn json_rpc_attestor(treestate_height: u32) -> (MockNode, JsonRpcAttestor) {
        let subtrees = json!({
            "pool": "sapling",
            "start_index": SUBTREE.0,
            "subtrees": [{ "root": hex::encode([1; 32]), "end_height": 1_999 }],
        });
        let node = MockNode::start(move |_, seen| match seen.body["method"].as_str().unwrap() {
            "getblockhash" => Reply::Result(json!(hash(2_999).to_string())),
            "z_gettreestate" => Reply::Result(treestate(treestate_height)),
            "z_getsubtreesbyindex" => Reply::Result(subtrees.clone()),
            method => panic!("unexpected method {method}"),
        })
        .await;
        let attestor =
            JsonRpcAttestor::new(ZakuraClient::new(RpcClient::new(node.config()).unwrap()));
        (node, attestor)
    }

    #[tokio::test]
    async fn json_rpc_attestor_reads_each_pool_in_its_byte_order() {
        let (node, attestor) = json_rpc_attestor(2_999).await;

        assert_eq!(attestor.tree_roots(2_999).await.unwrap(), roots());
        assert_eq!(attestor.block_hash(2_999).await.unwrap(), hash(2_999));
        assert_eq!(attestor.subtree_root(Pool::Sapling, SUBTREE).await.unwrap(), sapling_node(1));

        let calls = node.seen();
        assert_eq!(calls[1].body["params"], json!([2_999]));
        assert_eq!(calls[2].body["params"], json!(["sapling", 5, 1]));
    }

    #[tokio::test]
    async fn json_rpc_attestor_asks_the_node_once() {
        let (node, attestor) = json_rpc_attestor(2_999).await;

        for _ in 0..2 {
            attestor.tree_roots(2_999).await.unwrap();
            attestor.block_hash(2_999).await.unwrap();
            attestor.subtree_root(Pool::Sapling, SUBTREE).await.unwrap();
        }

        assert_eq!(node.seen().len(), 3);
    }

    #[tokio::test]
    async fn json_rpc_attestor_rejects_an_answer_for_another_height() {
        let (_node, attestor) = json_rpc_attestor(2_998).await;

        let error = attestor.tree_roots(2_999).await.unwrap_err();

        assert!(matches!(error, NodeError::Height { expected: 2_999, actual: 2_998 }), "{error:?}");
    }

    #[tokio::test]
    async fn json_rpc_attestor_names_a_missing_subtree() {
        let (_node, attestor) = json_rpc_attestor(2_999).await;
        let other = NoteCommitmentSubtreeIndex(6);

        let error = attestor.subtree_root(Pool::Sapling, other).await.unwrap_err();

        assert!(
            matches!(error, NodeError::MissingSubtree { pool: Pool::Sapling, index } if index == other),
            "{error:?}"
        );
    }
}
