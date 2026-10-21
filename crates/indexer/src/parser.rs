//! Selective parser for the wallet-scanning fields in Zcash transaction versions 1–6.

use serde::{Deserialize, Serialize};
use serde_big_array::BigArray;
use zakura_chain::{block, transaction};

use crate::{Ciphertext, Digest, EphemeralKey};

const MAX_TRANSACTION_BYTES: usize = 2_000_000;
const MAX_VECTOR_ITEMS: usize = u16::MAX as usize;
const MAX_EQUIHASH_SOLUTION_BYTES: usize = 1_344;
const HEADER_PREFIX_BYTES: usize = 140;
const COMPACT_CIPHERTEXT_BYTES: usize = 52;

const OVERWINTER_GROUP_ID: u32 = 0x03c4_8270;
const SAPLING_GROUP_ID: u32 = 0x892f_2085;
const V5_GROUP_ID: u32 = 0x26a7_270a;
const V6_GROUP_ID: u32 = 0xd884_b698;

const SAPLING_V4_SPEND_BYTES: usize = 384;
const SAPLING_SPEND_PREFIX_BYTES: usize = 96;
const SAPLING_OUTPUT_PREFIX_BYTES: usize = 756;
const SAPLING_V4_OUTPUT_BYTES: usize = 948;
const ORCHARD_ACTION_BYTES: usize = 820;
const BCTV14_JOINSPLIT_BYTES: usize = 1_802;
const GROTH16_JOINSPLIT_BYTES: usize = 1_698;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawIndexBlock {
    pub height: block::Height,
    pub hash: block::Hash,
    pub bytes: Vec<u8>,
    pub txids: Vec<transaction::Hash>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CompactSaplingOutput {
    pub cmu: Digest,
    pub ephemeral_key: EphemeralKey,
    #[serde(with = "BigArray")]
    pub ciphertext: Ciphertext,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CompactShieldedAction {
    pub nullifier: Digest,
    pub commitment: Digest,
    pub ephemeral_key: EphemeralKey,
    #[serde(with = "BigArray")]
    pub ciphertext: Ciphertext,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CompactTransaction {
    pub index: u64,
    pub txid: Digest,
    pub sapling_spends: Vec<Digest>,
    pub sapling_outputs: Vec<CompactSaplingOutput>,
    pub orchard_actions: Vec<CompactShieldedAction>,
    pub ironwood_actions: Vec<CompactShieldedAction>,
}

impl CompactTransaction {
    const fn has_payload(&self) -> bool {
        !self.sapling_spends.is_empty()
            || !self.sapling_outputs.is_empty()
            || !self.orchard_actions.is_empty()
            || !self.ironwood_actions.is_empty()
    }
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

#[derive(Clone, Copy, Debug, thiserror::Error)]
pub enum CompactParseError {
    #[error("block {height} has a truncated header")]
    TruncatedHeader { height: u32 },
    #[error("block {height} framing: {source}")]
    Framing {
        height: u32,
        #[source]
        source: TransactionParseError,
    },
    #[error("block {height} contains {transactions} transactions but Zakura supplied {txids} IDs")]
    TransactionCount { height: u32, transactions: usize, txids: usize },
    #[error("block {height} transaction {index}: {source}")]
    Transaction {
        height: u32,
        index: usize,
        #[source]
        source: TransactionParseError,
    },
    #[error("block {height} commitment count exceeds u32")]
    CommitmentCount { height: u32 },
}

#[derive(Clone, Copy, Debug, thiserror::Error)]
pub enum TransactionParseError {
    #[error("transaction is larger than {MAX_TRANSACTION_BYTES} bytes")]
    TooLarge,
    #[error("truncated transaction at byte {offset}")]
    Truncated { offset: usize },
    #[error("non-canonical CompactSize at byte {offset}")]
    NonCanonicalCompactSize { offset: usize },
    #[error("count or length at byte {offset} exceeds its bound")]
    LengthLimit { offset: usize },
    #[error("unsupported transaction header {header:#010x}")]
    UnsupportedHeader { header: u32 },
    #[error("wrong version group ID {actual:#010x}, expected {expected:#010x}")]
    VersionGroup { actual: u32, expected: u32 },
    #[error("trailing bytes at byte {offset}")]
    TrailingBytes { offset: usize },
}

pub fn parse_block(block: &RawIndexBlock) -> Result<ParsedCompactBlock, CompactParseError> {
    let height = block.height.0;
    let mut reader = Reader::new(&block.bytes);
    reader.skip(HEADER_PREFIX_BYTES).map_err(|_| CompactParseError::TruncatedHeader { height })?;
    let solution_bytes = reader
        .compact_size(MAX_EQUIHASH_SOLUTION_BYTES)
        .map_err(|source| CompactParseError::Framing { height, source })?;
    reader.skip(solution_bytes).map_err(|_| CompactParseError::TruncatedHeader { height })?;
    let transaction_count = reader
        .compact_size(MAX_VECTOR_ITEMS)
        .map_err(|source| CompactParseError::Framing { height, source })?;
    if transaction_count != block.txids.len() {
        return Err(CompactParseError::TransactionCount {
            height,
            transactions: transaction_count,
            txids: block.txids.len(),
        });
    }
    let previous_hash = block
        .bytes
        .get(4..36)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or(CompactParseError::TruncatedHeader { height })?;
    let time = u32::from_le_bytes(
        block
            .bytes
            .get(100..104)
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or(CompactParseError::TruncatedHeader { height })?,
    );

    let mut transactions = Vec::new();
    let mut sapling_additions = 0usize;
    let mut orchard_additions = 0usize;
    let mut ironwood_additions = 0usize;
    for (index, txid) in block.txids.iter().enumerate() {
        let start = reader.offset;
        let compact = parse_transaction_from(&mut reader, *txid, index as u64)
            .map_err(|source| CompactParseError::Transaction { height, index, source })?;
        if reader.offset - start > MAX_TRANSACTION_BYTES {
            return Err(CompactParseError::Transaction {
                height,
                index,
                source: TransactionParseError::TooLarge,
            });
        }
        sapling_additions = sapling_additions
            .checked_add(compact.sapling_outputs.len())
            .ok_or(CompactParseError::CommitmentCount { height })?;
        orchard_additions = orchard_additions
            .checked_add(compact.orchard_actions.len())
            .ok_or(CompactParseError::CommitmentCount { height })?;
        ironwood_additions = ironwood_additions
            .checked_add(compact.ironwood_actions.len())
            .ok_or(CompactParseError::CommitmentCount { height })?;
        if compact.has_payload() {
            transactions.push(compact);
        }
    }
    reader.finish().map_err(|source| CompactParseError::Framing { height, source })?;

    Ok(ParsedCompactBlock {
        height,
        hash: block.hash.0,
        previous_hash,
        time,
        transactions,
        sapling_additions: sapling_additions
            .try_into()
            .map_err(|_| CompactParseError::CommitmentCount { height })?,
        orchard_additions: orchard_additions
            .try_into()
            .map_err(|_| CompactParseError::CommitmentCount { height })?,
        ironwood_additions: ironwood_additions
            .try_into()
            .map_err(|_| CompactParseError::CommitmentCount { height })?,
    })
}

pub(crate) fn parse_stored_block<'a>(
    height: u32,
    hash: Digest,
    previous_hash: Digest,
    time: u32,
    transactions: impl IntoIterator<Item = (&'a [u8], transaction::Hash)>,
) -> Result<ParsedCompactBlock, CompactParseError> {
    let mut compact = Vec::new();
    let mut sapling_additions = 0usize;
    let mut orchard_additions = 0usize;
    let mut ironwood_additions = 0usize;
    for (index, (bytes, txid)) in transactions.into_iter().enumerate() {
        let transaction = parse_transaction(bytes, txid, index as u64)
            .map_err(|source| CompactParseError::Transaction { height, index, source })?;
        sapling_additions = sapling_additions
            .checked_add(transaction.sapling_outputs.len())
            .ok_or(CompactParseError::CommitmentCount { height })?;
        orchard_additions = orchard_additions
            .checked_add(transaction.orchard_actions.len())
            .ok_or(CompactParseError::CommitmentCount { height })?;
        ironwood_additions = ironwood_additions
            .checked_add(transaction.ironwood_actions.len())
            .ok_or(CompactParseError::CommitmentCount { height })?;
        if transaction.has_payload() {
            compact.push(transaction);
        }
    }
    Ok(ParsedCompactBlock {
        height,
        hash,
        previous_hash,
        time,
        transactions: compact,
        sapling_additions: sapling_additions
            .try_into()
            .map_err(|_| CompactParseError::CommitmentCount { height })?,
        orchard_additions: orchard_additions
            .try_into()
            .map_err(|_| CompactParseError::CommitmentCount { height })?,
        ironwood_additions: ironwood_additions
            .try_into()
            .map_err(|_| CompactParseError::CommitmentCount { height })?,
    })
}

pub fn parse_transaction(
    bytes: &[u8],
    txid: transaction::Hash,
    index: u64,
) -> Result<CompactTransaction, TransactionParseError> {
    if bytes.len() > MAX_TRANSACTION_BYTES {
        return Err(TransactionParseError::TooLarge);
    }

    let mut reader = Reader::new(bytes);
    let compact = parse_transaction_from(&mut reader, txid, index)?;
    reader.finish()?;
    Ok(compact)
}

fn parse_transaction_from(
    reader: &mut Reader<'_>,
    txid: transaction::Hash,
    index: u64,
) -> Result<CompactTransaction, TransactionParseError> {
    let header = reader.u32()?;
    let version = header & 0x7fff_ffff;
    let overwintered = header >> 31 != 0;
    let mut compact = CompactTransaction {
        index,
        txid: txid.0,
        sapling_spends: Vec::new(),
        sapling_outputs: Vec::new(),
        orchard_actions: Vec::new(),
        ironwood_actions: Vec::new(),
    };

    match (version, overwintered) {
        (1, false) => {
            reader.transparent_bundle()?;
            reader.skip(4)?;
        }
        (2, false) => {
            reader.transparent_bundle()?;
            reader.skip(4)?;
            reader.joinsplits(BCTV14_JOINSPLIT_BYTES)?;
        }
        (3, true) => {
            reader.version_group(OVERWINTER_GROUP_ID)?;
            reader.transparent_bundle()?;
            reader.skip(8)?;
            reader.joinsplits(BCTV14_JOINSPLIT_BYTES)?;
        }
        (4, true) => {
            reader.version_group(SAPLING_GROUP_ID)?;
            reader.transparent_bundle()?;
            reader.skip(16)?;
            (compact.sapling_spends, compact.sapling_outputs) = reader.sapling_v4()?;
        }
        (5, true) => {
            reader.version_group(V5_GROUP_ID)?;
            reader.skip(12)?;
            reader.transparent_bundle()?;
            (compact.sapling_spends, compact.sapling_outputs) = reader.sapling_v5()?;
            compact.orchard_actions = reader.actions()?;
        }
        (6, true) => {
            reader.version_group(V6_GROUP_ID)?;
            reader.skip(12)?;
            reader.transparent_bundle()?;
            (compact.sapling_spends, compact.sapling_outputs) = reader.sapling_v5()?;
            compact.orchard_actions = reader.actions()?;
            compact.ironwood_actions = reader.actions()?;
        }
        _ => return Err(TransactionParseError::UnsupportedHeader { header }),
    }

    Ok(compact)
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], TransactionParseError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or(TransactionParseError::LengthLimit { offset: self.offset })?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or(TransactionParseError::Truncated { offset: self.offset })?;
        self.offset = end;
        Ok(bytes)
    }

    fn skip(&mut self, len: usize) -> Result<(), TransactionParseError> {
        self.take(len).map(|_| ())
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], TransactionParseError> {
        Ok(self.take(N)?.try_into().expect("length was checked"))
    }

    fn u32(&mut self) -> Result<u32, TransactionParseError> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn compact_size(&mut self, max: usize) -> Result<usize, TransactionParseError> {
        let start = self.offset;
        let first = self.array::<1>()?[0];
        let value = match first {
            0..=252 => u64::from(first),
            253 => {
                let value = u16::from_le_bytes(self.array()?);
                if value < 253 {
                    return Err(TransactionParseError::NonCanonicalCompactSize { offset: start });
                }
                u64::from(value)
            }
            254 => {
                let value = self.u32()?;
                if value <= u32::from(u16::MAX) {
                    return Err(TransactionParseError::NonCanonicalCompactSize { offset: start });
                }
                u64::from(value)
            }
            255 => {
                let value = u64::from_le_bytes(self.array()?);
                if value <= u64::from(u32::MAX) {
                    return Err(TransactionParseError::NonCanonicalCompactSize { offset: start });
                }
                value
            }
        };
        let value = usize::try_from(value)
            .ok()
            .filter(|value| *value <= max)
            .ok_or(TransactionParseError::LengthLimit { offset: start })?;
        Ok(value)
    }

    fn count(&mut self) -> Result<usize, TransactionParseError> {
        self.compact_size(MAX_VECTOR_ITEMS)
    }

    fn skip_items(&mut self, count: usize, size: usize) -> Result<(), TransactionParseError> {
        let bytes = count
            .checked_mul(size)
            .ok_or(TransactionParseError::LengthLimit { offset: self.offset })?;
        self.skip(bytes)
    }

    fn ensure_items(&self, count: usize, size: usize) -> Result<(), TransactionParseError> {
        let bytes = count
            .checked_mul(size)
            .ok_or(TransactionParseError::LengthLimit { offset: self.offset })?;
        if bytes > self.bytes.len() - self.offset {
            return Err(TransactionParseError::Truncated { offset: self.offset });
        }
        Ok(())
    }

    fn version_group(&mut self, expected: u32) -> Result<(), TransactionParseError> {
        let actual = self.u32()?;
        if actual != expected {
            return Err(TransactionParseError::VersionGroup { actual, expected });
        }
        Ok(())
    }

    fn transparent_bundle(&mut self) -> Result<(), TransactionParseError> {
        let inputs = self.count()?;
        for _ in 0..inputs {
            self.skip(36)?;
            let script = self.compact_size(MAX_TRANSACTION_BYTES)?;
            self.skip(script)?;
            self.skip(4)?;
        }
        let outputs = self.count()?;
        for _ in 0..outputs {
            self.skip(8)?;
            let script = self.compact_size(MAX_TRANSACTION_BYTES)?;
            self.skip(script)?;
        }
        Ok(())
    }

    fn joinsplits(&mut self, item_size: usize) -> Result<(), TransactionParseError> {
        let count = self.count()?;
        self.skip_items(count, item_size)?;
        if count > 0 {
            self.skip(96)?;
        }
        Ok(())
    }

    fn sapling_v4(
        &mut self,
    ) -> Result<(Vec<Digest>, Vec<CompactSaplingOutput>), TransactionParseError> {
        let spend_count = self.count()?;
        self.ensure_items(spend_count, SAPLING_V4_SPEND_BYTES)?;
        let mut spends = Vec::with_capacity(spend_count);
        for _ in 0..spend_count {
            self.skip(64)?;
            spends.push(self.array()?);
            self.skip(SAPLING_V4_SPEND_BYTES - 96)?;
        }

        let output_count = self.count()?;
        self.ensure_items(output_count, SAPLING_V4_OUTPUT_BYTES)?;
        let mut outputs = Vec::with_capacity(output_count);
        for _ in 0..output_count {
            outputs.push(self.sapling_output(true)?);
        }

        self.joinsplits(GROTH16_JOINSPLIT_BYTES)?;
        if spend_count > 0 || output_count > 0 {
            self.skip(64)?;
        }
        Ok((spends, outputs))
    }

    fn sapling_v5(
        &mut self,
    ) -> Result<(Vec<Digest>, Vec<CompactSaplingOutput>), TransactionParseError> {
        let spend_count = self.count()?;
        self.ensure_items(spend_count, SAPLING_SPEND_PREFIX_BYTES)?;
        let mut spends = Vec::with_capacity(spend_count);
        for _ in 0..spend_count {
            self.skip(32)?;
            spends.push(self.array()?);
            self.skip(SAPLING_SPEND_PREFIX_BYTES - 64)?;
        }

        let output_count = self.count()?;
        self.ensure_items(output_count, SAPLING_OUTPUT_PREFIX_BYTES)?;
        let mut outputs = Vec::with_capacity(output_count);
        for _ in 0..output_count {
            outputs.push(self.sapling_output(false)?);
        }

        if spend_count > 0 || output_count > 0 {
            self.skip(8)?;
            if spend_count > 0 {
                self.skip(32)?;
            }
            self.skip_items(spend_count, 192 + 64)?;
            self.skip_items(output_count, 192)?;
            self.skip(64)?;
        }
        Ok((spends, outputs))
    }

    fn sapling_output(
        &mut self,
        includes_proof: bool,
    ) -> Result<CompactSaplingOutput, TransactionParseError> {
        self.skip(32)?;
        let cmu = self.array()?;
        let ephemeral_key = self.array()?;
        let ciphertext = self.array()?;
        self.skip(580 - COMPACT_CIPHERTEXT_BYTES + 80)?;
        if includes_proof {
            self.skip(SAPLING_V4_OUTPUT_BYTES - SAPLING_OUTPUT_PREFIX_BYTES)?;
        }
        Ok(CompactSaplingOutput { cmu, ephemeral_key, ciphertext })
    }

    fn actions(&mut self) -> Result<Vec<CompactShieldedAction>, TransactionParseError> {
        let count = self.count()?;
        self.ensure_items(count, ORCHARD_ACTION_BYTES)?;
        let mut actions = Vec::with_capacity(count);
        for _ in 0..count {
            self.skip(32)?;
            let nullifier = self.array()?;
            self.skip(32)?;
            let commitment = self.array()?;
            let ephemeral_key = self.array()?;
            let ciphertext = self.array()?;
            self.skip(580 - COMPACT_CIPHERTEXT_BYTES + 80)?;
            actions.push(CompactShieldedAction {
                nullifier,
                commitment,
                ephemeral_key,
                ciphertext,
            });
        }

        if count > 0 {
            self.skip(1 + 8 + 32)?;
            let proof = self.compact_size(MAX_TRANSACTION_BYTES)?;
            self.skip(proof)?;
            self.skip_items(count, 64)?;
            self.skip(64)?;
        }
        Ok(actions)
    }

    const fn finish(&self) -> Result<(), TransactionParseError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(TransactionParseError::TrailingBytes { offset: self.offset })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zakura_chain::{
        block::{Block, Height},
        parameters::NetworkUpgrade,
        serialization::{ZcashDeserialize as _, ZcashSerialize as _},
        transaction::{LockTime, Transaction},
    };
    use zakura_test::vectors::{
        BLOCK_MAINNET_949496_BYTES, BLOCK_MAINNET_1687106_BYTES, BLOCK_TESTNET_1842421_BYTES,
    };

    #[test]
    fn selective_parser_matches_full_zakura_parse() {
        let mut saw_sapling = false;
        let mut saw_sapling_spend = false;
        let mut saw_orchard = false;

        for encoded_block in [
            &*BLOCK_MAINNET_949496_BYTES,
            &*BLOCK_MAINNET_1687106_BYTES,
            &*BLOCK_TESTNET_1842421_BYTES,
        ] {
            let bytes = encoded_block.to_vec();
            let block = Block::zcash_deserialize(bytes.as_slice()).unwrap();
            let height = block.coinbase_height().unwrap();
            let parsed = parse_block(&RawIndexBlock {
                height,
                hash: block.hash(),
                bytes,
                txids: block.transactions.iter().map(|transaction| transaction.hash()).collect(),
            })
            .unwrap();
            let transaction_bytes = block
                .transactions
                .iter()
                .map(|transaction| transaction.zcash_serialize_to_vec().unwrap())
                .collect::<Vec<_>>();
            let stored = parse_stored_block(
                height.0,
                block.hash().0,
                block.header.previous_block_hash.0,
                u32::try_from(block.header.time.timestamp()).unwrap(),
                transaction_bytes
                    .iter()
                    .zip(&block.transactions)
                    .map(|(bytes, transaction)| (bytes.as_slice(), transaction.hash())),
            )
            .unwrap();
            assert_eq!(stored, parsed);
            let expected_transactions = block
                .transactions
                .iter()
                .enumerate()
                .map(|(index, transaction)| reference(transaction, index as u64))
                .filter(CompactTransaction::has_payload)
                .collect::<Vec<_>>();
            assert_eq!(parsed.transactions, expected_transactions);
            for (index, transaction) in block.transactions.iter().enumerate() {
                let bytes = transaction.zcash_serialize_to_vec().unwrap();
                let actual = parse_transaction(&bytes, transaction.hash(), index as u64).unwrap();
                let expected = reference(transaction, index as u64);
                saw_sapling |= !expected.sapling_outputs.is_empty();
                saw_sapling_spend |= !expected.sapling_spends.is_empty();
                saw_orchard |= !expected.orchard_actions.is_empty();
                assert_eq!(actual, expected);
            }
        }

        assert!(saw_sapling && saw_sapling_spend && saw_orchard);
    }

    #[test]
    fn v6_keeps_orchard_and_ironwood_separate() {
        let block = Block::zcash_deserialize(BLOCK_TESTNET_1842421_BYTES.as_slice()).unwrap();
        let shielded = block
            .transactions
            .iter()
            .find_map(|transaction| transaction.orchard_shielded_data().cloned())
            .unwrap();
        let transaction = Transaction::V6 {
            network_upgrade: NetworkUpgrade::Nu6_3,
            lock_time: LockTime::unlocked(),
            expiry_height: Height(1),
            inputs: Vec::new(),
            outputs: Vec::new(),
            sapling_shielded_data: None,
            orchard_shielded_data: Some(shielded.clone()),
            ironwood_shielded_data: Some(shielded),
        };
        let bytes = transaction.zcash_serialize_to_vec().unwrap();
        let parsed = Transaction::zcash_deserialize(bytes.as_slice()).unwrap();
        assert_eq!(parse_transaction(&bytes, parsed.hash(), 0).unwrap(), reference(&parsed, 0));
    }

    #[test]
    fn malformed_lengths_are_bounded() {
        let mut bytes = vec![1, 0, 0, 0, 0xff];
        bytes.extend_from_slice(&u64::MAX.to_le_bytes());
        assert!(matches!(
            parse_transaction(&bytes, zakura_chain::transaction::Hash([0; 32]), 0),
            Err(TransactionParseError::LengthLimit { .. })
        ));
    }

    fn reference(transaction: &Transaction, index: u64) -> CompactTransaction {
        let sapling_spends =
            transaction.sapling_spends_per_anchor().map(|spend| spend.nullifier.into()).collect();
        let sapling_outputs = transaction
            .sapling_outputs()
            .map(|output| {
                let encrypted: [u8; 580] = output.enc_ciphertext.into();
                CompactSaplingOutput {
                    cmu: output.cm_u.to_bytes(),
                    ephemeral_key: (&output.ephemeral_key).into(),
                    ciphertext: encrypted[..COMPACT_CIPHERTEXT_BYTES].try_into().unwrap(),
                }
            })
            .collect();
        let action = |action: &zakura_chain::orchard::Action| {
            let encrypted: [u8; 580] = action.enc_ciphertext.into();
            CompactShieldedAction {
                nullifier: action.nullifier.into(),
                commitment: action.cm_x.into(),
                ephemeral_key: (&action.ephemeral_key).into(),
                ciphertext: encrypted[..COMPACT_CIPHERTEXT_BYTES].try_into().unwrap(),
            }
        };

        CompactTransaction {
            index,
            txid: transaction.hash().0,
            sapling_spends,
            sapling_outputs,
            orchard_actions: transaction.orchard_actions().map(action).collect(),
            ironwood_actions: transaction.ironwood_actions().map(action).collect(),
        }
    }
}
