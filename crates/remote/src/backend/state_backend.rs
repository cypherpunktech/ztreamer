//! The [`StateBackend`] implementation of [`RemoteBackend`].

use std::{collections::HashSet, ops::RangeInclusive, sync::Arc};

use crate::RpcError;
use futures_util::{StreamExt as _, TryStreamExt as _, future, stream};
use sapling_crypto::Node as SaplingNode;
use zakura_chain::{
    amount::{Amount, NonNegative},
    block::{self, Block},
    ironwood, orchard,
    parameters::Network,
    serialization::ZcashSerialize as _,
    subtree::NoteCommitmentSubtreeIndex,
    transaction::{self, Transaction, UnminedTx},
    transparent,
};
use zakura_rpc::{
    client::{
        GetAddressBalanceRequest, GetAddressTxIdsRequest, GetAddressUtxosResponse,
        GetRawMempoolResponse,
    },
    methods::GetAddressUtxosRequest,
};
use zakura_state::HashOrHeight;
use ztreamer_backend::{
    AddressUtxo, BackendError, MinedTransaction, StateBackend, Subtrees, TreeState,
};

use super::{
    RemoteBackend,
    error::NodeCallError,
    node_bytes::FromNodeBytes,
    rpc_tree::RpcTree,
    shielded_pool::{IronwoodPool, OrchardPool, SaplingPool},
};

/// The maximum number of `getrawtransaction` calls at the same time for one mempool read.
const MEMPOOL_FETCHES: usize = 16;

#[tonic::async_trait]
impl StateBackend for RemoteBackend {
    fn network(&self) -> Network {
        self.network.clone()
    }

    async fn tip_height(&self) -> Result<Option<block::Height>, BackendError> {
        Ok(Some(self.node.get_block_count().await.map_err(NodeCallError)?))
    }

    async fn block(&self, hash: block::Hash) -> Result<Option<Arc<Block>>, BackendError> {
        let raw = match self.node.get_block_raw(HashOrHeight::Hash(hash)).await {
            Ok(raw) => raw,
            Err(error) if error.is_block_not_found() => return Ok(None),
            Err(error) => return Err(NodeCallError(error).into()),
        };
        let block = Block::from_node_bytes(raw.0).await?;
        if block.hash() != hash {
            return Err(BackendError::Unexpected(format!(
                "remote zakura sent block {} for {hash}",
                block.hash()
            )));
        }
        Ok(Some(Arc::new(block)))
    }

    async fn transaction(
        &self,
        hash: transaction::Hash,
    ) -> Result<Option<MinedTransaction>, BackendError> {
        let Some(fetched) = self.fetch_transaction(hash).await? else {
            return Ok(None);
        };
        let Some(height) = fetched.height() else {
            return Ok(None);
        };
        let transaction = Arc::new(fetched.decode().await?);
        Ok(Some(MinedTransaction { transaction, height }))
    }

    async fn tree_state(&self, hash: block::Hash) -> Result<TreeState, BackendError> {
        let cached = self.lock_tree_states().get(&hash).cloned();
        if let Some(trees) = cached {
            return Ok(trees);
        }
        let response =
            self.node.z_get_treestate(HashOrHeight::Hash(hash)).await.map_err(NodeCallError)?;
        if response.hash() != hash {
            return Err(BackendError::Unexpected(format!(
                "remote zakura sent the tree state of {} for {hash}",
                response.hash()
            )));
        }
        let span = tracing::Span::current();
        let trees = tokio::task::spawn_blocking(move || {
            span.in_scope(|| {
                Ok::<_, BackendError>(TreeState {
                    sapling: RpcTree::from_treestate("Sapling", response.sapling())?,
                    orchard: RpcTree::from_treestate("Orchard", response.orchard())?,
                    ironwood: RpcTree::from_treestate("Ironwood", response.ironwood())?,
                })
            })
        })
        .await
        .map_err(|error| BackendError::Unavailable(format!("decode task failed: {error}")))??;
        self.lock_tree_states().put(hash, trees.clone());
        Ok(trees)
    }

    async fn sapling_subtrees(
        &self,
        start: NoteCommitmentSubtreeIndex,
        limit: Option<NoteCommitmentSubtreeIndex>,
    ) -> Result<Subtrees<SaplingNode>, BackendError> {
        self.subtrees::<SaplingPool>(start, limit).await
    }

    async fn orchard_subtrees(
        &self,
        start: NoteCommitmentSubtreeIndex,
        limit: Option<NoteCommitmentSubtreeIndex>,
    ) -> Result<Subtrees<orchard::tree::Node>, BackendError> {
        self.subtrees::<OrchardPool>(start, limit).await
    }

    async fn ironwood_subtrees(
        &self,
        start: NoteCommitmentSubtreeIndex,
        limit: Option<NoteCommitmentSubtreeIndex>,
    ) -> Result<Subtrees<ironwood::tree::Node>, BackendError> {
        self.subtrees::<IronwoodPool>(start, limit).await
    }

    async fn address_transaction_ids(
        &self,
        address: transparent::Address,
        heights: RangeInclusive<block::Height>,
    ) -> Result<Vec<transaction::Hash>, BackendError> {
        // The node reads an end of 0 as the tip. The genesis block is not in the address index,
        // so a range that ends at it has no transactions.
        if heights.end().0 == 0 {
            return Ok(Vec::new());
        }
        // The node also changes a start above its tip to the tip. Thus read the tip first. The
        // tip of the node does not decrease, so the node then keeps the start. A start of 0
        // cannot be above the tip.
        if heights.start().0 > 0
            && *heights.start() > self.node.get_block_count().await.map_err(NodeCallError)?
        {
            return Ok(Vec::new());
        }
        let request = GetAddressTxIdsRequest::new(
            vec![address.to_string()],
            Some(heights.start().0),
            Some(heights.end().0),
        );
        self.node
            .get_address_tx_ids(&request)
            .await
            .map_err(NodeCallError)?
            .iter()
            .map(|id| {
                id.parse().map_err(|_| {
                    BackendError::Unexpected(format!("remote zakura sent a bad txid {id:?}"))
                })
            })
            .collect()
    }

    async fn address_balance(
        &self,
        addresses: HashSet<transparent::Address>,
    ) -> Result<Amount<NonNegative>, BackendError> {
        let request =
            GetAddressBalanceRequest::new(addresses.iter().map(ToString::to_string).collect());
        Ok(Amount::try_from(
            self.node.get_address_balance(&request).await.map_err(NodeCallError)?.balance(),
        )?)
    }

    async fn address_utxos(
        &self,
        addresses: HashSet<transparent::Address>,
    ) -> Result<Vec<AddressUtxo>, BackendError> {
        let addresses = addresses.iter().map(ToString::to_string).collect();
        let request = GetAddressUtxosRequest::new(addresses, false);
        let utxos = match self.node.get_address_utxos(&request).await.map_err(NodeCallError)? {
            GetAddressUtxosResponse::Utxos(utxos) => utxos,
            GetAddressUtxosResponse::UtxosAndChainInfo(object) => object.utxos().clone(),
        };
        utxos
            .iter()
            .map(|utxo| {
                let (address, hash, index, lock_script, zatoshis, height) = utxo.into_parts();
                Ok(AddressUtxo {
                    address,
                    outpoint: transparent::OutPoint { hash, index: index.index() },
                    output: transparent::Output { value: Amount::try_from(zatoshis)?, lock_script },
                    height,
                })
            })
            .collect()
    }

    async fn mempool_transactions(&self) -> Result<Vec<UnminedTx>, BackendError> {
        let ids = match self.node.get_raw_mempool().await.map_err(NodeCallError)? {
            GetRawMempoolResponse::TxIds(ids) => ids,
            GetRawMempoolResponse::Verbose(objects) => objects.into_keys().collect(),
        };
        let ids: Vec<transaction::Hash> = ids
            .iter()
            .map(|id| {
                id.parse().map_err(|_| {
                    BackendError::Unexpected(format!("remote zakura sent a bad mempool id {id:?}"))
                })
            })
            .collect::<Result<_, _>>()?;
        // The cache keeps only the transactions that the node still has in its mempool.
        let listed: HashSet<transaction::Hash> = ids.iter().copied().collect();
        self.lock_mempool().retain(|id, _| listed.contains(id));
        stream::iter(ids)
            .map(|id| async move {
                let fetched = self.mempool_transaction(id).await;
                (id, fetched)
            })
            .buffer_unordered(MEMPOOL_FETCHES)
            // A failed call for one transaction does not fail the read. That transaction is not
            // in the cache, so the next read requests it again.
            .filter_map(|(id, fetched)| {
                future::ready(match fetched {
                    Err(BackendError::Unavailable(error)) => {
                        tracing::warn!(txid = %id, %error, "remote zakura: skipped a mempool transaction");
                        None
                    }
                    fetched => fetched.transpose(),
                })
            })
            .try_collect()
            .await
    }

    async fn submit_transaction(
        &self,
        transaction: Transaction,
    ) -> Result<transaction::Hash, BackendError> {
        let bytes = transaction.zcash_serialize_to_vec().map_err(|error| {
            BackendError::Rejected(format!("transaction does not serialize: {error}"))
        })?;
        match self.node.send_raw_transaction(&bytes).await {
            Ok(sent) => Ok(sent.hash()),
            // Warm-up and a method that the listener does not serve are node errors, not
            // transaction errors.
            Err(error) if error.is_retryable() || error.is_method_not_found() => {
                Err(NodeCallError(error).into())
            }
            Err(RpcError::Rpc { code, message, .. }) => Err(BackendError::Rejected(format!(
                "remote zakura rejected transaction: {message} ({code})"
            ))),
            Err(error) => Err(NodeCallError(error).into()),
        }
    }
}
