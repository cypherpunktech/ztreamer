//! The embedded Zakura node as a [`StateBackend`]: [`InProcessBackend`].

use std::{collections::HashSet, ops::RangeInclusive, sync::Arc};

use sapling_crypto::Node as SaplingNode;
use tower::ServiceExt as _;
use zakura_chain::{
    amount::{Amount, NonNegative},
    block::{self, Block},
    ironwood, orchard,
    parameters::Network,
    subtree::NoteCommitmentSubtreeIndex,
    transaction::{self, Transaction, UnminedTx},
    transparent,
};
use zakura_state::{ReadRequest, ReadResponse, ReadStateService};
use ztreamer_backend::{
    AddressUtxo, BackendError, MinedTransaction, StateBackend, Subtrees, TreeState,
};

use crate::NodeClient;

/// The embedded Zakura node, reached in-process.
///
/// Built from a `ReadStateService` alone it serves chain reads and fails mempool requests;
/// built from a [`NodeClient`] it serves everything.
#[derive(Clone)]
pub struct InProcessBackend {
    read: ReadStateService,
    node: Option<NodeClient>,
}

impl std::fmt::Debug for InProcessBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InProcessBackend")
            .field("node", &self.node.is_some())
            .finish_non_exhaustive()
    }
}

impl InProcessBackend {
    /// Chain reads only. Mempool requests fail with [`BackendError::Unavailable`].
    pub const fn new(read: ReadStateService) -> Self {
        Self { read, node: None }
    }

    /// Chain reads and mempool access through the node's handles.
    pub fn with_node(node: NodeClient) -> Self {
        Self { read: node.read_state(), node: Some(node) }
    }

    fn node(&self) -> Result<&NodeClient, BackendError> {
        self.node.as_ref().ok_or_else(|| {
            BackendError::Unavailable("embedded Zakura node handle is unavailable".into())
        })
    }

    async fn read(&self, request: ReadRequest) -> Result<ReadResponse, BackendError> {
        self.read
            .clone()
            .oneshot(request)
            .await
            .map_err(|error| BackendError::Unavailable(format!("Zakura read failed: {error}")))
    }
}

fn unexpected(what: &str) -> BackendError {
    BackendError::Unexpected(format!("unexpected {what} response"))
}

/// Zakura's mempool futures are not `Send`, so they run on a blocking thread with their own
/// `block_on`.
async fn block_on_node<T, E, F>(call: F) -> Result<T, BackendError>
where
    F: FnOnce() -> Result<T, E> + Send + 'static,
    T: Send + 'static,
    E: std::fmt::Display + Send + 'static,
{
    tokio::task::spawn_blocking(call)
        .await
        .map_err(|error| BackendError::Unavailable(format!("Zakura task failed: {error}")))?
        .map_err(|error| BackendError::Unavailable(error.to_string()))
}

#[tonic::async_trait]
impl StateBackend for InProcessBackend {
    fn network(&self) -> Network {
        self.read.db().network()
    }

    async fn tip_height(&self) -> Result<Option<block::Height>, BackendError> {
        match self.read(ReadRequest::Tip).await? {
            ReadResponse::Tip(tip) => Ok(tip.map(|(height, _)| height)),
            _ => Err(unexpected("tip")),
        }
    }

    async fn block(&self, hash: block::Hash) -> Result<Option<Arc<Block>>, BackendError> {
        match self.read(ReadRequest::Block(hash.into())).await? {
            ReadResponse::Block(block) => Ok(block),
            _ => Err(unexpected("block")),
        }
    }

    async fn transaction(
        &self,
        hash: transaction::Hash,
    ) -> Result<Option<MinedTransaction>, BackendError> {
        match self.read(ReadRequest::Transaction(hash)).await? {
            ReadResponse::Transaction(Some(mined)) => {
                Ok(Some(MinedTransaction { transaction: mined.tx, height: mined.height }))
            }
            ReadResponse::Transaction(None) => Ok(None),
            _ => Err(unexpected("transaction")),
        }
    }

    async fn tree_state(&self, hash: block::Hash) -> Result<TreeState, BackendError> {
        let (sapling, orchard, ironwood) = tokio::try_join!(
            self.read(ReadRequest::SaplingTree(hash.into())),
            self.read(ReadRequest::OrchardTree(hash.into())),
            self.read(ReadRequest::IronwoodTree(hash.into())),
        )?;
        let ReadResponse::SaplingTree(sapling) = sapling else {
            return Err(unexpected("Sapling tree"));
        };
        let ReadResponse::OrchardTree(orchard) = orchard else {
            return Err(unexpected("Orchard tree"));
        };
        let ReadResponse::IronwoodTree(ironwood) = ironwood else {
            return Err(unexpected("Ironwood tree"));
        };
        Ok(TreeState { sapling, orchard, ironwood })
    }

    async fn sapling_subtrees(
        &self,
        start_index: NoteCommitmentSubtreeIndex,
        limit: Option<NoteCommitmentSubtreeIndex>,
    ) -> Result<Subtrees<SaplingNode>, BackendError> {
        match self.read(ReadRequest::SaplingSubtrees { start_index, limit }).await? {
            ReadResponse::SaplingSubtrees(subtrees) => Ok(subtrees),
            _ => Err(unexpected("Sapling subtree")),
        }
    }

    async fn orchard_subtrees(
        &self,
        start_index: NoteCommitmentSubtreeIndex,
        limit: Option<NoteCommitmentSubtreeIndex>,
    ) -> Result<Subtrees<orchard::tree::Node>, BackendError> {
        match self.read(ReadRequest::OrchardSubtrees { start_index, limit }).await? {
            ReadResponse::OrchardSubtrees(subtrees) => Ok(subtrees),
            _ => Err(unexpected("Orchard subtree")),
        }
    }

    async fn ironwood_subtrees(
        &self,
        start_index: NoteCommitmentSubtreeIndex,
        limit: Option<NoteCommitmentSubtreeIndex>,
    ) -> Result<Subtrees<ironwood::tree::Node>, BackendError> {
        match self.read(ReadRequest::IronwoodSubtrees { start_index, limit }).await? {
            ReadResponse::IronwoodSubtrees(subtrees) => Ok(subtrees),
            _ => Err(unexpected("Ironwood subtree")),
        }
    }

    async fn address_transaction_ids(
        &self,
        address: transparent::Address,
        heights: RangeInclusive<block::Height>,
    ) -> Result<Vec<transaction::Hash>, BackendError> {
        let request = ReadRequest::TransactionIdsByAddresses {
            addresses: HashSet::from([address]),
            height_range: heights,
        };
        match self.read(request).await? {
            ReadResponse::AddressesTransactionIds(ids) => Ok(ids.into_values().collect()),
            _ => Err(unexpected("address transaction")),
        }
    }

    async fn address_balance(
        &self,
        addresses: HashSet<transparent::Address>,
    ) -> Result<Amount<NonNegative>, BackendError> {
        match self.read(ReadRequest::AddressBalance(addresses)).await? {
            ReadResponse::AddressBalance { balance, .. } => Ok(balance),
            _ => Err(unexpected("address balance")),
        }
    }

    async fn address_utxos(
        &self,
        addresses: HashSet<transparent::Address>,
    ) -> Result<Vec<AddressUtxo>, BackendError> {
        match self.read(ReadRequest::UtxosByAddresses(addresses)).await? {
            ReadResponse::AddressUtxos(utxos) => Ok(utxos
                .utxos()
                .map(|(address, txid, location, output)| AddressUtxo {
                    address,
                    outpoint: transparent::OutPoint {
                        hash: *txid,
                        index: location.output_index().index(),
                    },
                    output: output.clone(),
                    height: location.height(),
                })
                .collect()),
            _ => Err(unexpected("address UTXO")),
        }
    }

    async fn mempool_transactions(&self) -> Result<Vec<UnminedTx>, BackendError> {
        let node = self.node()?.clone();
        let runtime = tokio::runtime::Handle::current();
        block_on_node(move || {
            runtime
                .block_on(node.mempool_transactions())
                .map_err(|error| format!("Zakura mempool query failed: {error}"))
        })
        .await
    }

    async fn submit_transaction(
        &self,
        transaction: Transaction,
    ) -> Result<transaction::Hash, BackendError> {
        let node = self.node()?.clone();
        let runtime = tokio::runtime::Handle::current();
        let submitted = tokio::task::spawn_blocking(move || {
            runtime
                .block_on(node.submit_transaction(transaction))
                .map_err(|error| error.to_string())
        })
        .await
        .map_err(|error| BackendError::Unavailable(format!("Zakura task failed: {error}")))?;
        submitted.map(|id| id.mined_id()).map_err(|error| {
            BackendError::Rejected(format!("Zakura rejected transaction: {error}"))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn read_only_backend_has_no_mempool() {
        let (_state, read, _tip, _change) = zakura_state::init(
            zakura_state::Config::ephemeral(),
            &Network::Mainnet,
            block::Height::MAX,
            0,
        )
        .await
        .expect("ephemeral state initializes");
        let backend = InProcessBackend::new(read);
        assert_eq!(backend.network(), Network::Mainnet);
        assert_eq!(backend.tip_height().await.unwrap(), None);
        let error = backend.mempool_transactions().await.unwrap_err();
        assert_eq!(
            error,
            BackendError::Unavailable("embedded Zakura node handle is unavailable".into())
        );
    }
}
