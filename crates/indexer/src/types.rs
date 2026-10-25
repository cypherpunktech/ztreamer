//! Compact block and transaction types produced by the parser and stored by the index.

use zakura_chain::{
    block::{self, Block},
    serialization::{SerializationError, ZcashDeserialize as _},
    transaction,
};

use crate::{Ciphertext, Digest, EphemeralKey};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawIndexBlock {
    pub height: block::Height,
    pub hash: block::Hash,
    pub bytes: Vec<u8>,
    pub txids: Vec<transaction::Hash>,
}

/// Why [`RawIndexBlock::from_bytes`] refused a serialized block.
#[derive(Debug, thiserror::Error)]
pub enum RawBlockError {
    /// The bytes are not a Zcash block.
    #[error("block could not be deserialized: {0}")]
    Deserialize(#[from] SerializationError),
    /// The block has no coinbase transaction with a height.
    #[error("coinbase height is missing")]
    CoinbaseHeight,
}

impl RawIndexBlock {
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
