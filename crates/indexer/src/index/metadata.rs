//! The metadata table: the persisted [`IndexState`] and the chain identity.
//!
//! The table has two rows. All integers are big-endian.
//!
//! `"state"`, 62 bytes. Each commit writes it:
//!
//! ```text
//! offset  size  field
//!      0     1  format version (1)
//!      1     4  durable tip height
//!      5    32  durable tip hash
//!     37     1  1 if sealed_through is set, else 0
//!     38     4  sealed_through (0 if not set)
//!     42     8  generation
//!     50     4  Sapling tree size
//!     54     4  Orchard tree size
//!     58     4  Ironwood tree size
//! ```
//!
//! A tree size is the number of note commitments in the pool, at the end of the durable tip.
//! An index with no commit has no state row, so a state row always has a durable tip. The decoder
//! rejects a row with a different length or version, a flag other than 0 or 1, or a
//! `sealed_through` that is above the tip or is not the last height of a range.
//!
//! `"identity"`. [`Index::open`](super::Index::open) writes it once, when it creates the index:
//!
//! ```text
//! SCHEMA_VERSION (4) | RANGE_SIZE (4) | PERSIST_DEPTH (4) | SEAL_DEPTH (4) |
//! genesis hash (32) | network name (UTF-8, to the end of the value)
//! ```
//!
//! [`Metadata::put_or_check_identity`] compares the stored identity with the identity of the
//! current build and chain. If they are different, `Index::open` fails with
//! [`IndexError::Identity`].

use heed::{Database, RoTxn, RwTxn, types::Bytes};
use zakura_chain::block;

use super::{
    BlockId, IndexError, IndexState, PERSIST_DEPTH, RANGE_SIZE, SCHEMA_VERSION, SEAL_DEPTH,
};
use crate::codec::TreeSizes;

const STATE: &[u8] = b"state";
const IDENTITY: &[u8] = b"identity";
const STATE_FORMAT_VERSION: u8 = 1;
const STATE_BYTES: usize = 62;

/// The metadata table. It holds the persisted [`IndexState`] and the chain identity, each under
/// a key that only this type knows.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Metadata(Database<Bytes, Bytes>);

impl Metadata {
    pub(crate) const fn new(db: Database<Bytes, Bytes>) -> Self {
        Self(db)
    }

    /// Reads the persisted state. An index that has never committed has the default state.
    pub(crate) fn get_state(&self, txn: &RoTxn<'_>) -> Result<IndexState, IndexError> {
        self.0.get(txn, STATE)?.map(decode_state).transpose().map(Option::unwrap_or_default)
    }

    pub(crate) fn put_state(
        &self,
        txn: &mut RwTxn<'_>,
        state: IndexState,
    ) -> Result<(), IndexError> {
        Ok(self.0.put(txn, STATE, encode_state(state).as_slice())?)
    }

    /// Stores the identity of a new index, and rejects an index made for another chain or with
    /// another fixed policy.
    pub(crate) fn put_or_check_identity(
        &self,
        txn: &mut RwTxn<'_>,
        network: &str,
        genesis_hash: block::Hash,
    ) -> Result<(), IndexError> {
        let identity = identity(network, genesis_hash);
        match self.0.get(txn, IDENTITY)? {
            Some(stored) if stored != identity => Err(IndexError::Identity),
            Some(_) => Ok(()),
            None => Ok(self.0.put(txn, IDENTITY, identity.as_slice())?),
        }
    }
}

fn encode_state(state: IndexState) -> Vec<u8> {
    let tip = state.durable_tip.expect("only non-empty index state is persisted");
    let mut bytes = Vec::with_capacity(STATE_BYTES);
    bytes.push(STATE_FORMAT_VERSION);
    bytes.extend_from_slice(&tip.height.to_be_bytes());
    bytes.extend_from_slice(&tip.hash.0);
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
        u32::from_be_bytes(
            bytes[start..start + 4].try_into().expect("decode_state checked the state length"),
        )
    };
    let tip = BlockId::new(
        u32_at(1),
        block::Hash(bytes[5..37].try_into().expect("decode_state checked the state length")),
    );
    let sealed_through = (bytes[37] == 1).then(|| u32_at(38));
    if sealed_through
        .is_some_and(|sealed| sealed > tip.height || sealed % RANGE_SIZE != RANGE_SIZE - 1)
    {
        return Err(IndexError::Metadata);
    }
    Ok(IndexState {
        durable_tip: Some(tip),
        sealed_through,
        generation: u64::from_be_bytes(
            bytes[42..50].try_into().expect("decode_state checked the state length"),
        ),
        tree_sizes: TreeSizes { sapling: u32_at(50), orchard: u32_at(54), ironwood: u32_at(58) },
    })
}

fn identity(network: &str, genesis_hash: block::Hash) -> Vec<u8> {
    [
        SCHEMA_VERSION.to_be_bytes().as_slice(),
        RANGE_SIZE.to_be_bytes().as_slice(),
        PERSIST_DEPTH.to_be_bytes().as_slice(),
        SEAL_DEPTH.to_be_bytes().as_slice(),
        genesis_hash.0.as_slice(),
        network.as_bytes(),
    ]
    .concat()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{index::Index, test_utils::hash};

    fn state(sealed_through: Option<u32>) -> IndexState {
        IndexState {
            durable_tip: Some(BlockId::new(1_050, hash(1_050))),
            sealed_through,
            generation: 7,
            tree_sizes: TreeSizes { sapling: 1, orchard: 2, ironwood: 3 },
        }
    }

    #[test]
    fn state_round_trips_and_defaults_when_absent() {
        let dir = tempfile::tempdir().unwrap();
        let index =
            Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap();
        let metadata = index.tables.metadata;
        let mut txn = index.env.write_txn().unwrap();
        assert_eq!(metadata.get_state(&txn).unwrap(), IndexState::default());

        for sealed_through in [None, Some(999)] {
            metadata.put_state(&mut txn, state(sealed_through)).unwrap();
            assert_eq!(metadata.get_state(&txn).unwrap(), state(sealed_through));
        }
    }

    #[test]
    fn state_rejects_a_seal_that_does_not_end_a_range() {
        let dir = tempfile::tempdir().unwrap();
        let index =
            Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap();
        let metadata = index.tables.metadata;
        let mut txn = index.env.write_txn().unwrap();

        metadata.put_state(&mut txn, state(Some(500))).unwrap();
        assert!(matches!(metadata.get_state(&txn), Err(IndexError::Metadata)));
    }

    #[test]
    fn identity_accepts_a_repeat_and_rejects_another_chain() {
        let dir = tempfile::tempdir().unwrap();
        let index =
            Index::open(dir.path(), 10 * 1024 * 1024, "Mainnet", block::Hash([1; 32])).unwrap();
        let metadata = index.tables.metadata;
        let mut txn = index.env.write_txn().unwrap();

        metadata.put_or_check_identity(&mut txn, "Mainnet", block::Hash([1; 32])).unwrap();
        assert!(matches!(
            metadata.put_or_check_identity(&mut txn, "Testnet", block::Hash([1; 32])),
            Err(IndexError::Identity)
        ));
        assert!(matches!(
            metadata.put_or_check_identity(&mut txn, "Mainnet", block::Hash([2; 32])),
            Err(IndexError::Identity)
        ));
    }
}
