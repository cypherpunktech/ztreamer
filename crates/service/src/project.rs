//! Compact index record projection, shielded-pool filtering, and protobuf response construction.

use std::collections::HashSet;

use tonic::Status;
use zakura_chain::{
    serialization::{BytesInDisplayOrder as _, ZcashDeserialize as _},
    transaction,
};
use ztreamer_indexer::{
    parser::parse_transaction,
    types::{CompactShieldedAction, CompactTransaction},
};
use ztreamer_protocol::proto;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PoolSelection {
    pub(crate) transparent: bool,
    sapling: bool,
    orchard: bool,
    ironwood: bool,
}

impl PoolSelection {
    pub(crate) const fn is_all(self) -> bool {
        !self.transparent && self.sapling && self.orchard && self.ironwood
    }

    pub(crate) fn from_request(pool_types: &[i32]) -> Result<Self, Status> {
        Self::from_mempool_request(pool_types)
    }

    /// Empty means every shielded pool, for compatibility with legacy clients.
    pub(crate) fn from_mempool_request(pool_types: &[i32]) -> Result<Self, Status> {
        if pool_types.is_empty() {
            return Ok(Self { transparent: false, sapling: true, orchard: true, ironwood: true });
        }

        let mut selection =
            Self { transparent: false, sapling: false, orchard: false, ironwood: false };
        for &pool_type in pool_types {
            match proto::PoolType::try_from(pool_type) {
                Ok(proto::PoolType::Sapling) => selection.sapling = true,
                Ok(proto::PoolType::Orchard) => selection.orchard = true,
                Ok(proto::PoolType::Ironwood) => selection.ironwood = true,
                Ok(proto::PoolType::Transparent) => selection.transparent = true,
                Ok(proto::PoolType::Invalid) | Err(_) => {
                    return Err(Status::invalid_argument(format!("invalid pool type {pool_type}")));
                }
            }
        }
        Ok(selection)
    }
}

/// Project in place: discarded pools need no second record/object graph.
pub(crate) fn project_block(
    mut block: proto::CompactBlock,
    pools: PoolSelection,
    nullifiers: bool,
) -> proto::CompactBlock {
    for tx in &mut block.vtx {
        if !pools.sapling {
            tx.spends.clear();
        }
        if !pools.sapling || nullifiers {
            tx.outputs.clear();
        }
        if !pools.orchard {
            tx.actions.clear();
        }
        if !pools.ironwood {
            tx.ironwood_actions.clear();
        }
        if nullifiers {
            for action in tx.actions.iter_mut().chain(&mut tx.ironwood_actions) {
                action.cmx.clear();
                action.ephemeral_key.clear();
                action.ciphertext.clear();
            }
        }
    }
    if nullifiers {
        block.chain_metadata = Some(proto::ChainMetadata::default());
    } else {
        block.vtx.retain(|tx| {
            !tx.spends.is_empty()
                || !tx.outputs.is_empty()
                || !tx.actions.is_empty()
                || !tx.ironwood_actions.is_empty()
        });
    }
    block
}

pub(crate) fn add_transparent_txs(
    block: &mut proto::CompactBlock,
    transactions: &[std::sync::Arc<transaction::Transaction>],
) {
    for (index, tx) in transactions.iter().enumerate() {
        let vin: Vec<_> = tx
            .spent_outpoints()
            .map(|outpoint| proto::CompactTxIn {
                prevout_txid: outpoint.hash.0.to_vec(),
                prevout_index: outpoint.index,
            })
            .collect();
        let vout: Vec<_> = tx
            .outputs()
            .iter()
            .map(|output| proto::TxOut {
                value: output.value.into(),
                script_pub_key: output.lock_script.as_raw_bytes().to_vec(),
            })
            .collect();
        if vin.is_empty() && vout.is_empty() {
            continue;
        }
        let index = index as u64;
        let position = match block.vtx.binary_search_by_key(&index, |tx| tx.index) {
            Ok(position) => position,
            Err(position) => {
                block.vtx.insert(
                    position,
                    proto::CompactTx { index, txid: tx.hash().0.to_vec(), ..Default::default() },
                );
                position
            }
        };
        block.vtx[position].vin = vin;
        block.vtx[position].vout = vout;
    }
}

pub(crate) fn compact_mempool_txs(
    transactions: &[(Vec<u8>, transaction::Hash)],
    pools: PoolSelection,
    exclude_txid_suffixes: &[Vec<u8>],
) -> Result<Vec<proto::CompactTx>, Status> {
    let excluded =
        excluded_txids(transactions.iter().map(|(_, txid)| *txid), exclude_txid_suffixes);
    let mut compact = Vec::with_capacity(transactions.len().saturating_sub(excluded.len()));
    for (bytes, txid) in transactions {
        if excluded.contains(txid) {
            continue;
        }
        let transaction = parse_transaction(bytes, *txid, 0).map_err(|error| {
            Status::internal(format!("mempool transaction {txid} is unparsable: {error}"))
        })?;
        let mut compact_tx = convert_transaction(&transaction, pools);
        if pools.transparent {
            let transaction = transaction::Transaction::zcash_deserialize(bytes.as_slice())
                .map_err(|error| Status::internal(error.to_string()))?;
            let vin: Vec<_> = transaction
                .spent_outpoints()
                .map(|outpoint| proto::CompactTxIn {
                    prevout_txid: outpoint.hash.0.to_vec(),
                    prevout_index: outpoint.index,
                })
                .collect();
            let vout: Vec<_> = transaction
                .outputs()
                .iter()
                .map(|output| proto::TxOut {
                    value: output.value.into(),
                    script_pub_key: output.lock_script.as_raw_bytes().to_vec(),
                })
                .collect();
            if !vin.is_empty() || !vout.is_empty() {
                let compact_tx = compact_tx.get_or_insert_with(|| proto::CompactTx {
                    txid: txid.0.to_vec(),
                    ..Default::default()
                });
                compact_tx.vin = vin;
                compact_tx.vout = vout;
            }
        }
        if let Some(transaction) = compact_tx {
            compact.push(transaction);
        }
    }
    Ok(compact)
}

pub(crate) fn excluded_txids(
    txids: impl Iterator<Item = transaction::Hash>,
    suffixes: &[Vec<u8>],
) -> HashSet<transaction::Hash> {
    let mut excluded = HashSet::new();
    if suffixes.is_empty() {
        return excluded;
    }
    // A suffix in protocol order is a prefix in display order. After the sort, the txids
    // that match a prefix are adjacent.
    let mut displayed: Vec<_> = txids.map(|txid| txid.bytes_in_display_order()).collect();
    displayed.sort_unstable();
    for suffix in suffixes.iter().filter(|suffix| suffix.len() <= 32) {
        let prefix: Vec<_> = suffix.iter().rev().copied().collect();
        let start = displayed.partition_point(|txid| txid.as_slice() < prefix.as_slice());
        if let Some(txid) = displayed.get(start).filter(|txid| txid.starts_with(&prefix))
            && !displayed.get(start + 1).is_some_and(|next| next.starts_with(&prefix))
        {
            excluded.insert(transaction::Hash::from_bytes_in_display_order(txid));
        }
    }
    excluded
}

fn convert_transaction(
    transaction: &CompactTransaction,
    pools: PoolSelection,
) -> Option<proto::CompactTx> {
    let spends = if pools.sapling {
        transaction
            .sapling_spends
            .iter()
            .map(|nullifier| proto::CompactSaplingSpend { nf: nullifier.to_vec() })
            .collect()
    } else {
        Vec::new()
    };
    let outputs = if pools.sapling {
        transaction
            .sapling_outputs
            .iter()
            .map(|output| proto::CompactSaplingOutput {
                cmu: output.cmu.to_vec(),
                ephemeral_key: output.ephemeral_key.to_vec(),
                ciphertext: output.ciphertext.to_vec(),
            })
            .collect()
    } else {
        Vec::new()
    };
    let actions =
        if pools.orchard { convert_actions(&transaction.orchard_actions) } else { Vec::new() };
    let ironwood_actions =
        if pools.ironwood { convert_actions(&transaction.ironwood_actions) } else { Vec::new() };

    (!spends.is_empty()
        || !outputs.is_empty()
        || !actions.is_empty()
        || !ironwood_actions.is_empty())
    .then(|| proto::CompactTx {
        index: transaction.index,
        txid: transaction.txid.0.to_vec(),
        fee: 0,
        spends,
        outputs,
        actions,
        ironwood_actions,
        vin: Vec::new(),
        vout: Vec::new(),
    })
}

fn convert_actions(actions: &[CompactShieldedAction]) -> Vec<proto::CompactOrchardAction> {
    actions
        .iter()
        .map(|action| proto::CompactOrchardAction {
            nullifier: action.nullifier.to_vec(),
            cmx: action.commitment.to_vec(),
            ephemeral_key: action.ephemeral_key.to_vec(),
            ciphertext: action.ciphertext.to_vec(),
        })
        .collect()
}

#[cfg(test)]
mod tests;
