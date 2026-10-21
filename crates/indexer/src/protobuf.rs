//! Lossless conversion between index records and the existing compact-block schema.

use ztreamer_protocol::{compact_bytes, proto};

use crate::{
    codec::{CodecError, CompactBlockRecord, TreeSizes},
    parser::{CompactSaplingOutput, CompactShieldedAction, CompactTransaction},
};

impl CompactBlockRecord {
    pub fn to_proto(&self) -> proto::CompactBlock {
        proto::CompactBlock {
            height: self.height.into(),
            hash: self.hash.to_vec(),
            prev_hash: self.previous_hash.to_vec(),
            time: self.time,
            header: Vec::new(),
            vtx: self.transactions.iter().map(transaction).collect(),
            chain_metadata: Some(proto::ChainMetadata {
                sapling_commitment_tree_size: self.end_tree_sizes.sapling,
                orchard_commitment_tree_size: self.end_tree_sizes.orchard,
                ironwood_commitment_tree_size: self.end_tree_sizes.ironwood,
            }),
        }
    }

    pub(crate) fn from_proto(block: compact_bytes::CompactBlock) -> Result<Self, CodecError> {
        let trees = block.chain_metadata.ok_or(CodecError::InvalidRecord)?;
        Ok(Self {
            height: block.height.try_into().map_err(|_| CodecError::InvalidRecord)?,
            hash: array(block.hash)?,
            previous_hash: array(block.prev_hash)?,
            time: block.time,
            transactions: block
                .vtx
                .into_iter()
                .map(|tx| {
                    Ok(CompactTransaction {
                        index: tx.index,
                        txid: array(tx.txid)?,
                        sapling_spends: tx
                            .spends
                            .into_iter()
                            .map(|spend| array(spend.nf))
                            .collect::<Result<_, _>>()?,
                        sapling_outputs: tx
                            .outputs
                            .into_iter()
                            .map(|output| {
                                Ok(CompactSaplingOutput {
                                    cmu: array(output.cmu)?,
                                    ephemeral_key: array(output.ephemeral_key)?,
                                    ciphertext: array(output.ciphertext)?,
                                })
                            })
                            .collect::<Result<_, CodecError>>()?,
                        orchard_actions: tx
                            .actions
                            .into_iter()
                            .map(decode_action)
                            .collect::<Result<_, _>>()?,
                        ironwood_actions: tx
                            .ironwood_actions
                            .into_iter()
                            .map(decode_action)
                            .collect::<Result<_, _>>()?,
                    })
                })
                .collect::<Result<_, CodecError>>()?,
            end_tree_sizes: TreeSizes {
                sapling: trees.sapling_commitment_tree_size,
                orchard: trees.orchard_commitment_tree_size,
                ironwood: trees.ironwood_commitment_tree_size,
            },
        })
    }
}

pub(crate) fn transaction(tx: &CompactTransaction) -> proto::CompactTx {
    proto::CompactTx {
        index: tx.index,
        txid: tx.txid.to_vec(),
        spends: tx
            .sapling_spends
            .iter()
            .map(|nf| proto::CompactSaplingSpend { nf: nf.to_vec() })
            .collect(),
        outputs: tx
            .sapling_outputs
            .iter()
            .map(|output| proto::CompactSaplingOutput {
                cmu: output.cmu.to_vec(),
                ephemeral_key: output.ephemeral_key.to_vec(),
                ciphertext: output.ciphertext.to_vec(),
            })
            .collect(),
        actions: tx.orchard_actions.iter().map(action).collect(),
        ironwood_actions: tx.ironwood_actions.iter().map(action).collect(),
        ..Default::default()
    }
}

fn action(action: &CompactShieldedAction) -> proto::CompactOrchardAction {
    proto::CompactOrchardAction {
        nullifier: action.nullifier.to_vec(),
        cmx: action.commitment.to_vec(),
        ephemeral_key: action.ephemeral_key.to_vec(),
        ciphertext: action.ciphertext.to_vec(),
    }
}

fn decode_action(
    action: compact_bytes::CompactOrchardAction,
) -> Result<CompactShieldedAction, CodecError> {
    Ok(CompactShieldedAction {
        nullifier: array(action.nullifier)?,
        commitment: array(action.cmx)?,
        ephemeral_key: array(action.ephemeral_key)?,
        ciphertext: array(action.ciphertext)?,
    })
}

fn array<const N: usize>(bytes: impl AsRef<[u8]>) -> Result<[u8; N], CodecError> {
    bytes.as_ref().try_into().map_err(|_| CodecError::InvalidRecord)
}
