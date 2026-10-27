//! Compact block and transaction types produced by the parser and stored by the index.

use std::{io, panic};

use zakura_chain::{
    block::{self, Block},
    serialization::{SerializationError, ZcashDeserialize as _, ZcashSerialize as _},
    transaction,
};
use zakura_state::HashOrHeight;
use ztreamer_remote::{RpcError, ZakuraClient};

use crate::{
    Ciphertext, Digest, EphemeralKey,
    codec::{CompactBlockRecord, TreeSizes},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawIndexBlock {
    pub height: block::Height,
    pub hash: block::Hash,
    pub bytes: Vec<u8>,
    pub txids: Vec<transaction::Hash>,
}

/// Why [`RawIndexBlock::from_bytes`] refused a serialized block, or why
/// [`RawIndexBlock::try_from`] refused a decoded block.
#[derive(Debug, thiserror::Error)]
pub enum RawBlockError {
    /// The bytes are not a Zcash block.
    #[error("block could not be deserialized: {0}")]
    Deserialize(#[from] SerializationError),
    /// The block has no coinbase transaction with a height.
    #[error("coinbase height is missing")]
    CoinbaseHeight,
    /// The serialization of the block failed.
    #[error(transparent)]
    Serialize(io::Error),
}

/// Why [`RawIndexBlock::fetch_at_height`] or [`RawIndexBlock::from_bytes_at_height`] gave no
/// block.
#[derive(Debug, thiserror::Error)]
pub enum BlockAtHeightError {
    /// The `getblock` call failed. [`RpcError::is_block_not_found`] is `true` when the node has
    /// no block at the height.
    #[error(transparent)]
    Rpc(#[from] RpcError),
    /// The bytes of the block at `height` do not decode.
    #[error("block at height {height} does not decode")]
    Decode { height: u32, source: RawBlockError },
    /// The block for height `expected` has the coinbase height `actual`.
    #[error("block for height {expected} is at height {actual}")]
    Height { expected: u32, actual: u32 },
}

impl RawIndexBlock {
    /// The best-chain block at `height` from one `getblock` call with verbosity 0. It decodes
    /// the block on the blocking thread pool, with [`Self::from_bytes_at_height`].
    pub async fn fetch_at_height(
        node: &ZakuraClient,
        height: u32,
    ) -> Result<Self, BlockAtHeightError> {
        let bytes = node.get_block_raw(HashOrHeight::Height(block::Height(height))).await?.0;
        tokio::task::spawn_blocking(move || Self::from_bytes_at_height(bytes, height))
            .await
            .unwrap_or_else(|error| panic::resume_unwind(error.into_panic()))
    }

    /// Decodes `bytes` with [`Self::from_bytes`], and refuses a block that is not at `height`.
    pub fn from_bytes_at_height(bytes: Vec<u8>, height: u32) -> Result<Self, BlockAtHeightError> {
        let block = Self::from_bytes(bytes)
            .map_err(|source| BlockAtHeightError::Decode { height, source })?;
        if block.height.0 != height {
            return Err(BlockAtHeightError::Height { expected: height, actual: block.height.0 });
        }
        Ok(block)
    }

    /// Decodes a serialized block, for example the result of `getblock` with verbosity 0 from a
    /// remote node.
    ///
    /// The height comes from the coinbase input, the hash from the header and the transaction
    /// ids from the transactions. `bytes` stays as it is. The compact parser examines it again
    /// and refuses trailing bytes.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, RawBlockError> {
        let block = Block::zcash_deserialize(bytes.as_slice())?;
        let height = block.coinbase_height().ok_or(RawBlockError::CoinbaseHeight)?;
        let txids = block.transactions.iter().map(|transaction| transaction.hash()).collect();
        Ok(Self { height, hash: block.hash(), bytes, txids })
    }
}

impl TryFrom<&Block> for RawIndexBlock {
    type Error = RawBlockError;

    /// Serializes a decoded block, for example a block from the embedded node.
    ///
    /// The height comes from the coinbase input, the hash from the header and the transaction
    /// ids from the transactions.
    fn try_from(block: &Block) -> Result<Self, RawBlockError> {
        let hash = block.hash();
        let height = block.coinbase_height().ok_or(RawBlockError::CoinbaseHeight)?;
        let bytes = block.zcash_serialize_to_vec().map_err(RawBlockError::Serialize)?;
        let txids = block.transactions.iter().map(|transaction| transaction.hash()).collect();
        Ok(Self { height, hash, bytes, txids })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompactSaplingOutput {
    pub cmu: Digest,
    pub ephemeral_key: EphemeralKey,
    pub ciphertext: Ciphertext,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompactShieldedAction {
    pub nullifier: Digest,
    pub commitment: Digest,
    pub ephemeral_key: EphemeralKey,
    pub ciphertext: Ciphertext,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactTransaction {
    pub index: u64,
    pub txid: transaction::Hash,
    pub sapling_spends: Vec<Digest>,
    pub sapling_outputs: Vec<CompactSaplingOutput>,
    pub orchard_actions: Vec<CompactShieldedAction>,
    pub ironwood_actions: Vec<CompactShieldedAction>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Parser output that later gets converted into a
/// [`crate::codec::CompactBlockRecord`]
pub struct ParsedCompactBlock {
    pub height: u32,
    pub hash: block::Hash,
    pub previous_hash: block::Hash,
    pub time: u32,
    pub transactions: Vec<CompactTransaction>,
    pub sapling_additions: u32,
    pub orchard_additions: u32,
    pub ironwood_additions: u32,
}

impl ParsedCompactBlock {
    /// The record that the index stores for this block, after the tree sizes `before` the block.
    /// Returns `None` if a tree size overflows.
    pub fn into_record(self, before: TreeSizes) -> Option<CompactBlockRecord> {
        let additions = TreeSizes {
            sapling: self.sapling_additions,
            orchard: self.orchard_additions,
            ironwood: self.ironwood_additions,
        };
        Some(CompactBlockRecord {
            height: self.height,
            hash: self.hash,
            previous_hash: self.previous_hash,
            time: self.time,
            transactions: self.transactions,
            end_tree_sizes: before.checked_add(additions)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{mixed_records, parsed};

    #[test]
    fn a_parsed_block_becomes_its_record() {
        let records = mixed_records(10);
        let before = records[8].end_tree_sizes;

        let record = parsed(records[9].clone()).into_record(before).unwrap();

        assert_eq!(record, records[9]);
    }

    #[test]
    fn a_tree_size_overflow_gives_no_record() {
        let record = mixed_records(10).swap_remove(9);
        assert!(record.additions().sapling > 0);
        let before = TreeSizes { sapling: u32::MAX, ..TreeSizes::default() };

        assert!(parsed(record).into_record(before).is_none());
    }
}
