//! Read note commitment trees and tree roots from the `zcashd` RPC encoding.
//!
//! To verify a sealed range, a replica rebuilds the note commitment trees. Then it compares their
//! roots with the roots from a node. `z_gettreestate` gives each tree as `finalState` and each
//! root as `finalRoot`. `finalState` is the hex of the bytes that
//! [`sapling::tree::NoteCommitmentTree::to_rpc_bytes`] writes. [`FromRpc`] reads these values
//! into the zakura types. Compare the zakura types, not the hex strings.
//!
//! # Byte order
//!
//! The node prints a Sapling root in reversed byte order. It prints an Orchard root in the stored
//! byte order. `bytes_in_display_order` on each zakura `Root` uses the same order. Ironwood uses
//! the Orchard tree types (`ironwood::tree` is `orchard::tree`). Because of this, an Ironwood root
//! uses the Orchard byte order.

use std::io;

use incrementalmerkletree::{Hashable, frontier::Frontier};
use zakura_chain::{orchard, sapling, serialization::SerializationError};
use zcash_primitives::merkle_tree::{HashSer, read_commitment_tree};

/// An error from [`FromRpc`].
#[derive(Debug, thiserror::Error)]
pub enum RpcDecodeError {
    /// The root does not have 32 bytes.
    #[error("tree root has {0} bytes, not 32")]
    RootLength(usize),
    /// The root bytes are not a valid field element.
    #[error("tree root is not valid")]
    Root(#[from] SerializationError),
    /// The commitment tree encoding is not valid.
    #[error("commitment tree encoding is not valid")]
    Tree(#[from] io::Error),
    /// Bytes remain after the commitment tree encoding.
    #[error("commitment tree encoding has {0} trailing bytes")]
    TrailingBytes(usize),
}

/// Reads a value from its `zcashd` RPC encoding.
///
/// # Implementation
///
/// Implement [`FromRpc::from_rpc_bytes`] for the bytes that the RPC prints as hex.
///
/// # Example
///
/// ```
/// use zakura_chain::sapling;
/// use ztreamer_indexer::frontier::FromRpc;
///
/// // The Sapling `finalRoot` at height 3,524,999, as the node prints it.
/// let printed = "63a3da9f0249167a8c04e45c43dcb62145d78dd0917c2d2f2f49414696e00c0b";
/// let root = sapling::tree::Root::from_rpc_bytes(&hex::decode(printed).unwrap()).unwrap();
///
/// assert_eq!(hex::encode(root.bytes_in_display_order()), printed);
/// ```
pub trait FromRpc: Sized {
    /// Reads the value from the RPC bytes.
    fn from_rpc_bytes(bytes: &[u8]) -> Result<Self, RpcDecodeError>;
}

impl FromRpc for sapling::tree::Root {
    /// Reverses the bytes, because the node prints a Sapling root in reversed byte order.
    fn from_rpc_bytes(bytes: &[u8]) -> Result<Self, RpcDecodeError> {
        let mut bytes = root_bytes(bytes)?;
        bytes.reverse();
        Ok(Self::try_from(bytes)?)
    }
}

impl FromRpc for orchard::tree::Root {
    /// Keeps the byte order. This also reads Ironwood roots.
    fn from_rpc_bytes(bytes: &[u8]) -> Result<Self, RpcDecodeError> {
        Ok(Self::try_from(root_bytes(bytes)?)?)
    }
}

impl FromRpc for sapling::tree::NoteCommitmentTree {
    /// Reads the tree that [`sapling::tree::NoteCommitmentTree::to_rpc_bytes`] writes.
    ///
    /// This function does not verify the tree. Before you use the tree, compare its root with a
    /// root from a trusted source.
    fn from_rpc_bytes(bytes: &[u8]) -> Result<Self, RpcDecodeError> {
        read_frontier(bytes).map(Self::from_frontier)
    }
}

impl FromRpc for orchard::tree::NoteCommitmentTree {
    /// Reads the tree that [`orchard::tree::NoteCommitmentTree::to_rpc_bytes`] writes. This
    /// also reads Ironwood trees.
    ///
    /// This function does not verify the tree. Before you use the tree, compare its root with a
    /// root from a trusted source.
    fn from_rpc_bytes(bytes: &[u8]) -> Result<Self, RpcDecodeError> {
        read_frontier(bytes).map(Self::from_frontier)
    }
}

fn root_bytes(bytes: &[u8]) -> Result<[u8; 32], RpcDecodeError> {
    bytes.try_into().map_err(|_| RpcDecodeError::RootLength(bytes.len()))
}

/// Reads the `zcashd` commitment tree encoding into a frontier. Returns an error if bytes remain.
fn read_frontier<H, const DEPTH: u8>(mut bytes: &[u8]) -> Result<Frontier<H, DEPTH>, RpcDecodeError>
where
    H: HashSer + Hashable + Clone,
{
    let tree = read_commitment_tree::<H, _, DEPTH>(&mut bytes)?;
    match bytes.len() {
        0 => Ok(tree.to_frontier()),
        trailing => Err(RpcDecodeError::TrailingBytes(trailing)),
    }
}

#[cfg(test)]
mod tests {
    use zakura_chain::{block, ironwood};

    use super::*;

    /// The rebuilt roots at each sealed range end, in the stored byte order.
    const ROOTS_CSV: &str =
        include_str!("../../../benchmarks/2026-10-22-replication/out/roots.csv");

    /// Reads a value from its RPC hex, as the node prints it.
    fn from_hex<T: FromRpc>(hex: &str) -> Result<T, RpcDecodeError> {
        T::from_rpc_bytes(&hex::decode(hex).unwrap())
    }

    /// The `finalRoot` values at height 3,524,999, as the node prints them. From
    /// `benchmarks/2026-10-22-replication/out/z_gettreestate-3524999-sample.txt`.
    const NODE_SAPLING_ROOT: &str =
        "63a3da9f0249167a8c04e45c43dcb62145d78dd0917c2d2f2f49414696e00c0b";
    const NODE_ORCHARD_ROOT: &str =
        "a106f539e6eed51e3ecfa332fbe7eefca819bbccad4ccbcf2abadbbd421b6522";
    const NODE_IRONWOOD_ROOT: &str =
        "f27ecd88bfbe5198a92a440e8778e558d17f0cc6d754d404f550bdc8d529272d";

    /// The tree lengths for the round trips. The tree encoding has different fields for an
    /// empty tree, an odd length and an even length.
    const LENGTHS: [u8; 5] = [0, 1, 2, 5, 8];

    struct MeasuredRoots {
        end_height: block::Height,
        sapling: sapling::tree::Root,
        orchard: orchard::tree::Root,
    }

    fn measured_roots() -> Vec<MeasuredRoots> {
        let mut lines = ROOTS_CSV.lines();
        let header: Vec<_> = lines.next().unwrap().split(',').collect();
        let column = |name| header.iter().position(|&c| c == name).unwrap();
        let (end_height, sapling, orchard) =
            (column("end_height"), column("sapling_root"), column("orchard_root"));
        let stored = |hex| <[u8; 32]>::try_from(hex::decode(hex).unwrap()).unwrap();

        lines
            .map(|line| line.split(',').collect::<Vec<_>>())
            .map(|row| MeasuredRoots {
                end_height: block::Height(row[end_height].parse().unwrap()),
                sapling: sapling::tree::Root::try_from(stored(row[sapling])).unwrap(),
                orchard: orchard::tree::Root::try_from(stored(row[orchard])).unwrap(),
            })
            .collect()
    }

    fn sapling_tree(len: u8) -> sapling::tree::NoteCommitmentTree {
        (1..=len)
            .map(|i| {
                let mut bytes = [0; 32];
                bytes[0] = i;
                sapling::tree::NoteCommitmentUpdate::from_bytes(&bytes).unwrap()
            })
            .collect::<Vec<_>>()
            .into()
    }

    fn orchard_tree(len: u8) -> orchard::tree::NoteCommitmentTree {
        (1..=len)
            .map(|i| orchard::tree::NoteCommitmentUpdate::from(u64::from(i)))
            .collect::<Vec<_>>()
            .into()
    }

    #[test]
    fn sapling_tree_round_trips_through_rpc_bytes() {
        for len in LENGTHS {
            let tree = sapling_tree(len);
            let parsed =
                sapling::tree::NoteCommitmentTree::from_rpc_bytes(&tree.to_rpc_bytes()).unwrap();

            assert_eq!(parsed, tree);
            assert_eq!(parsed.root(), tree.root());
        }
    }

    #[test]
    fn orchard_tree_round_trips_through_rpc_hex() {
        for len in LENGTHS {
            let tree = orchard_tree(len);
            let parsed =
                from_hex::<orchard::tree::NoteCommitmentTree>(&hex::encode(tree.to_rpc_bytes()))
                    .unwrap();

            assert_eq!(parsed, tree);
            assert_eq!(parsed.root(), tree.root());
        }
    }

    #[test]
    fn tree_with_trailing_bytes_is_rejected() {
        let mut bytes = sapling_tree(3).to_rpc_bytes();
        bytes.push(0);

        let error = sapling::tree::NoteCommitmentTree::from_rpc_bytes(&bytes).unwrap_err();

        assert!(matches!(error, RpcDecodeError::TrailingBytes(1)), "{error:?}");
    }

    #[test]
    fn root_with_wrong_length_is_rejected() {
        let error = orchard::tree::Root::from_rpc_bytes(&[0; 31]).unwrap_err();

        assert!(matches!(error, RpcDecodeError::RootLength(31)), "{error:?}");
    }

    #[test]
    fn roots_round_trip_through_display_order() {
        let sapling = sapling_tree(5).root();
        let orchard = orchard_tree(5).root();

        assert_eq!(
            sapling::tree::Root::from_rpc_bytes(&sapling.bytes_in_display_order()).unwrap(),
            sapling
        );
        assert_eq!(
            orchard::tree::Root::from_rpc_bytes(&orchard.bytes_in_display_order()).unwrap(),
            orchard
        );
    }

    #[test]
    fn measured_roots_parse_in_the_byte_order_of_each_pool() {
        let roots = measured_roots();
        assert_eq!(roots.len(), 3_525);

        for MeasuredRoots { end_height, sapling, orchard } in roots {
            let sapling_stored = hex::encode(<[u8; 32]>::from(sapling));
            let sapling_printed = hex::encode(sapling.bytes_in_display_order());
            let orchard_stored = hex::encode(<[u8; 32]>::from(orchard));
            let mut orchard_reversed = <[u8; 32]>::from(orchard);
            orchard_reversed.reverse();
            let orchard_reversed = hex::encode(orchard_reversed);

            // The node prints Sapling reversed (`match-reversed`) and Orchard as stored (`match`).
            assert_eq!(
                from_hex::<sapling::tree::Root>(&sapling_printed).unwrap(),
                sapling,
                "{end_height:?}"
            );
            assert_eq!(
                from_hex::<orchard::tree::Root>(&orchard_stored).unwrap(),
                orchard,
                "{end_height:?}"
            );

            // The other byte order does not give the same root.
            assert!(
                from_hex::<sapling::tree::Root>(&sapling_stored).ok() != Some(sapling),
                "{end_height:?}"
            );
            assert!(
                from_hex::<orchard::tree::Root>(&orchard_reversed).ok() != Some(orchard),
                "{end_height:?}"
            );
        }
    }

    #[test]
    fn node_roots_equal_rebuilt_roots_at_3_524_999() {
        let rebuilt = measured_roots()
            .into_iter()
            .find(|roots| roots.end_height == block::Height(3_524_999))
            .unwrap();

        assert_eq!(from_hex::<sapling::tree::Root>(NODE_SAPLING_ROOT).unwrap(), rebuilt.sapling);
        assert_eq!(from_hex::<orchard::tree::Root>(NODE_ORCHARD_ROOT).unwrap(), rebuilt.orchard);
    }

    #[test]
    fn ironwood_root_parses_with_the_orchard_code_path() {
        // `ironwood::tree` is `orchard::tree`, so this is the Orchard implementation.
        let ironwood: orchard::tree::Root =
            from_hex::<ironwood::tree::Root>(NODE_IRONWOOD_ROOT).unwrap();

        assert_eq!(hex::encode(ironwood.bytes_in_display_order()), NODE_IRONWOOD_ROOT);
        assert_ne!(ironwood, from_hex::<orchard::tree::Root>(NODE_ORCHARD_ROOT).unwrap());
    }
}
