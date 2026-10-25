//! The metadata table: the persisted [`IndexState`] and the chain identity.
//!
//! The table has two rows. All integers are big-endian.
//!
//! `"state"`, 62 or 67 bytes. Each commit writes it:
//!
//! ```text
//! offset  size  field
//!      0     1  format version (1 or 2)
//!      1     4  durable tip height
//!      5    32  durable tip hash
//!     37     1  1 if sealed_through is set, else 0
//!     38     4  sealed_through (0 if not set)
//!     42     8  generation
//!     50     4  Sapling tree size
//!     54     4  Orchard tree size
//!     58     4  Ironwood tree size
//!     62     1  version 2 only: 1 if verified_through is set, else 0
//!     63     4  version 2 only: verified_through (0 if not set)
//! ```
//!
//! A tree size is the number of note commitments in the pool, at the end of the durable tip.
//! An index with no commit has no state row, so a state row always has a durable tip. A state
//! without `verified_through` is written in version 1, so an index that embedded mode writes
//! stays readable by a build from before version 2. The decoder rejects a row with a length that
//! does not match its version, an unknown version, a flag other than 0 or 1, a `sealed_through`
//! that is above the tip or is not the last height of a range, or a `verified_through` that is
//! above `sealed_through` or is not the last height of a range.
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
const STATE_V1: u8 = 1;
const STATE_V2: u8 = 2;
const STATE_V1_BYTES: usize = 62;
const STATE_V2_BYTES: usize = 67;

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
    let mut bytes = Vec::with_capacity(STATE_V2_BYTES);
    bytes.push(if state.verified_through.is_some() { STATE_V2 } else { STATE_V1 });
    bytes.extend_from_slice(&tip.height.to_be_bytes());
    bytes.extend_from_slice(&tip.hash.0);
    bytes.push(u8::from(state.sealed_through.is_some()));
    bytes.extend_from_slice(&state.sealed_through.unwrap_or_default().to_be_bytes());
    bytes.extend_from_slice(&state.generation.to_be_bytes());
    bytes.extend_from_slice(&state.tree_sizes.sapling.to_be_bytes());
    bytes.extend_from_slice(&state.tree_sizes.orchard.to_be_bytes());
    bytes.extend_from_slice(&state.tree_sizes.ironwood.to_be_bytes());
    if let Some(verified_through) = state.verified_through {
        bytes.push(1);
        bytes.extend_from_slice(&verified_through.to_be_bytes());
    }
    bytes
}

fn decode_state(bytes: &[u8]) -> Result<IndexState, IndexError> {
    let expected_len = match bytes.first() {
        Some(&STATE_V1) => STATE_V1_BYTES,
        Some(&STATE_V2) => STATE_V2_BYTES,
        _ => return Err(IndexError::Metadata),
    };
    if bytes.len() != expected_len || bytes[37] > 1 {
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
    let verified_through = match bytes.get(62) {
        None | Some(0) => None,
        Some(1) => Some(u32_at(63)),
        Some(_) => return Err(IndexError::Metadata),
    };
    if verified_through.is_some_and(|verified| {
        sealed_through.is_none_or(|sealed| verified > sealed)
            || verified % RANGE_SIZE != RANGE_SIZE - 1
    }) {
        return Err(IndexError::Metadata);
    }
    Ok(IndexState {
        durable_tip: Some(tip),
        sealed_through,
        verified_through,
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
            durable_tip: Some(BlockId::new(2_050, hash(2_050))),
            sealed_through,
            verified_through: None,
            generation: 7,
            tree_sizes: TreeSizes { sapling: 1, orchard: 2, ironwood: 3 },
        }
    }

    fn verified_state(verified_through: Option<u32>) -> IndexState {
        IndexState { verified_through, ..state(Some(1_999)) }
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
        for verified_through in [None, Some(999), Some(1_999)] {
            metadata.put_state(&mut txn, verified_state(verified_through)).unwrap();
            assert_eq!(metadata.get_state(&txn).unwrap(), verified_state(verified_through));
        }
    }

    /// A state without `verified_through` is written exactly as before version 2, and a version
    /// 1 row from an older build decodes with no `verified_through`.
    #[test]
    fn state_without_a_verified_prefix_keeps_the_version_1_row() {
        let encoded = encode_state(state(Some(999)));
        assert_eq!(encoded.len(), STATE_V1_BYTES);
        assert_eq!(encoded[0], STATE_V1);
        assert_eq!(decode_state(&encoded).unwrap(), state(Some(999)));

        let encoded = encode_state(verified_state(Some(999)));
        assert_eq!(encoded.len(), STATE_V2_BYTES);
        assert_eq!(encoded[0], STATE_V2);
        assert_eq!(&encoded[62..], &[1, 0, 0, 3, 231]);
    }

    #[test]
    fn state_rejects_a_bad_version_length_or_verified_prefix() {
        let v1 = encode_state(state(Some(999)));
        let v2 = encode_state(verified_state(Some(999)));

        let mut other_version = v1.clone();
        other_version[0] = 3;
        assert!(matches!(decode_state(&other_version), Err(IndexError::Metadata)));

        let mut short_v2 = v2.clone();
        short_v2.truncate(STATE_V1_BYTES);
        assert!(matches!(decode_state(&short_v2), Err(IndexError::Metadata)));

        let mut long_v1 = v1;
        long_v1.extend_from_slice(&v2[62..]);
        assert!(matches!(decode_state(&long_v1), Err(IndexError::Metadata)));

        let mut bad_flag = v2;
        bad_flag[62] = 2;
        assert!(matches!(decode_state(&bad_flag), Err(IndexError::Metadata)));

        // Above sealed_through, not the end of a range, or without any sealed range.
        for verified in [Some(2_999), Some(500)] {
            assert!(matches!(
                decode_state(&encode_state(verified_state(verified))),
                Err(IndexError::Metadata)
            ));
        }
        let unsealed = IndexState { sealed_through: None, ..verified_state(Some(999)) };
        assert!(matches!(decode_state(&encode_state(unsealed)), Err(IndexError::Metadata)));
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
