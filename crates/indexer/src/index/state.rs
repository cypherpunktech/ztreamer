//! Encoding of the index's metadata records: the persisted [`IndexState`] and the chain identity.

use heed::{Database, RoTxn, types::Bytes};

use super::{
    BlockId, IndexError, IndexState, PERSIST_DEPTH, RANGE_SIZE, SCHEMA_VERSION, SEAL_DEPTH, STATE,
};
use crate::{Digest, codec::TreeSizes};

const STATE_FORMAT_VERSION: u8 = 1;
const STATE_BYTES: usize = 62;

pub(crate) fn read_state(
    metadata: Database<Bytes, Bytes>,
    txn: &RoTxn<'_>,
) -> Result<IndexState, IndexError> {
    metadata.get(txn, STATE)?.map(decode_state).transpose().map(Option::unwrap_or_default)
}

pub(crate) fn encode_state(state: IndexState) -> Vec<u8> {
    let tip = state.durable_tip.expect("only non-empty index state is persisted");
    let mut bytes = Vec::with_capacity(STATE_BYTES);
    bytes.push(STATE_FORMAT_VERSION);
    bytes.extend_from_slice(&tip.height.to_be_bytes());
    bytes.extend_from_slice(&tip.hash);
    bytes.push(u8::from(state.sealed_through.is_some()));
    bytes.extend_from_slice(&state.sealed_through.unwrap_or_default().to_be_bytes());
    bytes.extend_from_slice(&state.generation.to_be_bytes());
    bytes.extend_from_slice(&state.tree_sizes.sapling.to_be_bytes());
    bytes.extend_from_slice(&state.tree_sizes.orchard.to_be_bytes());
    bytes.extend_from_slice(&state.tree_sizes.ironwood.to_be_bytes());
    bytes
}

fn decode_state(bytes: &[u8]) -> Result<IndexState, IndexError> {
    if bytes.len() != STATE_BYTES || bytes[0] != STATE_FORMAT_VERSION || bytes[37] > 1 {
        return Err(IndexError::Metadata);
    }
    let u32_at = |start| {
        u32::from_be_bytes(bytes[start..start + 4].try_into().expect("state length was checked"))
    };
    let tip = BlockId::new(u32_at(1), bytes[5..37].try_into().expect("state length was checked"));
    let sealed_through = (bytes[37] == 1).then(|| u32_at(38));
    if sealed_through
        .is_some_and(|sealed| sealed > tip.height || sealed % RANGE_SIZE != RANGE_SIZE - 1)
    {
        return Err(IndexError::Metadata);
    }
    Ok(IndexState {
        durable_tip: Some(tip),
        sealed_through,
        generation: u64::from_be_bytes(bytes[42..50].try_into().expect("state length was checked")),
        tree_sizes: TreeSizes { sapling: u32_at(50), orchard: u32_at(54), ironwood: u32_at(58) },
    })
}

pub(crate) fn identity(network: &str, genesis_hash: Digest) -> Vec<u8> {
    [
        SCHEMA_VERSION.to_be_bytes().as_slice(),
        RANGE_SIZE.to_be_bytes().as_slice(),
        PERSIST_DEPTH.to_be_bytes().as_slice(),
        SEAL_DEPTH.to_be_bytes().as_slice(),
        genesis_hash.as_slice(),
        network.as_bytes(),
    ]
    .concat()
}
