//! Mempool requests: submitting transactions, streaming the mempool, and compact mempool
//! transactions.

use std::{collections::HashSet, sync::Arc, time::Duration};

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;
use zakura_chain::{
    serialization::{ZcashDeserialize, ZcashSerialize},
    transaction::{self, Transaction},
};
use ztreamer_protocol::proto;

use super::{CompactService, RpcStream};
use crate::project::{PoolSelection, compact_mempool_txs, excluded_txids};

impl CompactService {
    pub(crate) async fn send_transaction(
        &self,
        request: proto::RawTransaction,
    ) -> Result<proto::SendResponse, Status> {
        let transaction = Transaction::zcash_deserialize(request.data.as_slice())
            .map_err(|error| Status::invalid_argument(format!("invalid transaction: {error}")))?;
        let txid = self.backend.submit_transaction(transaction).await?;
        Ok(proto::SendResponse { error_code: 0, error_message: txid.to_string() })
    }

    pub(crate) async fn mempool_stream(&self) -> Result<RpcStream<proto::RawTransaction>, Status> {
        let backend = Arc::clone(&self.backend);
        let mut tip_changes = backend.tip_listener()?;
        let mut transactions = backend.mempool_transactions().await?;
        let (sender, receiver) = mpsc::channel(1);
        tokio::spawn(async move {
            let mut seen = HashSet::new();
            loop {
                for transaction in transactions.drain(..) {
                    if !seen.insert(transaction.id()) {
                        continue;
                    }
                    let transaction = match transaction.transaction().zcash_serialize_to_vec() {
                        Ok(data) => proto::RawTransaction { data, height: 0 },
                        Err(error) => {
                            let _ = sender.send(Err(Status::internal(error.to_string()))).await;
                            return;
                        }
                    };
                    tokio::select! {
                        biased;
                        change = tip_changes.tip_changed() => {
                            if let Err(error) = change {
                                let _ = sender.send(Err(Status::from(error))).await;
                            }
                            return;
                        }
                        result = sender.send(Ok(transaction)) => {
                            if result.is_err() {
                                return;
                            }
                        }
                    }
                }

                transactions = tokio::select! {
                    biased;
                    change = tip_changes.tip_changed() => {
                        if let Err(error) = change {
                            let _ = sender.send(Err(Status::from(error))).await;
                        }
                        return;
                    }
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {
                        match backend.mempool_transactions().await {
                            Ok(transactions) => transactions,
                            Err(error) => {
                                let _ = sender.send(Err(Status::from(error))).await;
                                return;
                            }
                        }
                    }
                };
            }
        });
        Ok(Box::pin(ReceiverStream::new(receiver)))
    }

    pub(crate) async fn mempool_tx(
        &self,
        request: proto::GetMempoolTxRequest,
    ) -> Result<RpcStream<proto::CompactTx>, Status> {
        PoolSelection::from_mempool_request(&request.pool_types)?;
        let transactions = self.backend.mempool_transactions().await?;
        let compact = tokio::task::spawn_blocking(move || mempool_compact(transactions, &request))
            .await
            .map_err(|error| Status::internal(format!("mempool conversion failed: {error}")))??;
        Ok(Box::pin(tokio_stream::iter(compact.into_iter().map(Ok))))
    }
}

fn mempool_compact(
    transactions: Vec<transaction::UnminedTx>,
    request: &proto::GetMempoolTxRequest,
) -> Result<Vec<proto::CompactTx>, Status> {
    let pools = PoolSelection::from_mempool_request(&request.pool_types)?;
    let excluded = excluded_txids(
        transactions.iter().map(|tx| tx.id().mined_id()),
        &request.exclude_txid_suffixes,
    );
    let transactions = transactions
        .into_iter()
        .filter(|tx| !excluded.contains(&tx.id().mined_id()))
        .map(|transaction| {
            transaction
                .transaction()
                .zcash_serialize_to_vec()
                .map(|bytes| (bytes, transaction.id().mined_id()))
                .map_err(|error| Status::internal(error.to_string()))
        })
        .collect::<Result<Vec<_>, Status>>()?;
    compact_mempool_txs(&transactions, pools, &[])
}

#[cfg(test)]
mod tests {
    use super::*;
    use zakura_chain::block;
    use zakura_test::vectors::{BLOCK_MAINNET_949496_BYTES, BLOCK_TESTNET_1842421_BYTES};

    #[test]
    fn mempool_requests_select_their_pools_and_exclusions() {
        let sapling = shielded_mempool_transaction(&BLOCK_MAINNET_949496_BYTES, |transaction| {
            transaction.sapling_outputs().next().is_some()
        });
        let orchard = shielded_mempool_transaction(&BLOCK_TESTNET_1842421_BYTES, |transaction| {
            transaction.orchard_actions().next().is_some()
        });
        let sapling_txid = sapling.id().mined_id().0.to_vec();
        let orchard_txid = orchard.id().mined_id().0.to_vec();
        let mempool = vec![sapling, orchard];
        let txids = |compact: Vec<proto::CompactTx>| {
            compact.into_iter().map(|transaction| transaction.txid).collect::<Vec<_>>()
        };

        let compact =
            mempool_compact(mempool.clone(), &proto::GetMempoolTxRequest::default()).unwrap();
        assert_eq!(txids(compact.clone()), [sapling_txid.clone(), orchard_txid.clone()]);
        assert!(!compact[0].outputs.is_empty());
        assert!(!compact[1].actions.is_empty());

        let orchard_only = proto::GetMempoolTxRequest {
            pool_types: vec![proto::PoolType::Orchard as i32],
            ..Default::default()
        };
        assert_eq!(
            txids(mempool_compact(mempool.clone(), &orchard_only).unwrap()),
            vec![orchard_txid.clone()]
        );

        let excluded = proto::GetMempoolTxRequest {
            exclude_txid_suffixes: vec![orchard_txid[30..].to_vec()],
            ..Default::default()
        };
        assert_eq!(txids(mempool_compact(mempool.clone(), &excluded).unwrap()), [sapling_txid]);

        let transparent = proto::GetMempoolTxRequest {
            pool_types: vec![proto::PoolType::Transparent as i32],
            ..Default::default()
        };
        let compact = mempool_compact(mempool, &transparent).unwrap();
        assert!(!compact.is_empty());
        assert!(compact.iter().all(|tx| tx.spends.is_empty()
            && tx.outputs.is_empty()
            && tx.actions.is_empty()
            && tx.ironwood_actions.is_empty()));
    }

    fn shielded_mempool_transaction(
        block: &[u8],
        shielded: impl Fn(&transaction::Transaction) -> bool,
    ) -> transaction::UnminedTx {
        block::Block::zcash_deserialize(block)
            .unwrap()
            .transactions
            .iter()
            .find(|candidate| shielded(candidate))
            .cloned()
            .expect("the block vector holds a shielded transaction")
            .into()
    }
}
