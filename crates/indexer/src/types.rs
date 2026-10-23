//! Compact block and transaction types produced by the parser and stored by the index.

use zakura_chain::{block, transaction};

use crate::{Ciphertext, Digest, EphemeralKey};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawIndexBlock {
    pub height: block::Height,
    pub hash: block::Hash,
    pub bytes: Vec<u8>,
    pub txids: Vec<transaction::Hash>,
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
    pub txid: Digest,
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
    pub hash: Digest,
    pub previous_hash: Digest,
    pub time: u32,
    pub transactions: Vec<CompactTransaction>,
    pub sapling_additions: u32,
    pub orchard_additions: u32,
    pub ironwood_additions: u32,
}
