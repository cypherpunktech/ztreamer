//! The `range_meta` table: one [`RangeMeta`] record per sealed range.
//!
//! A record is what a replica needs to know about a range without decoding its blocks: the
//! manifest entry a snapshot publishes for it, how far the range has been verified against a
//! node, and the node's answers so that they are never asked for twice. Embedded mode never
//! writes this table; a filler task and the importer do.
//!
//! # Layout
//!
//! All integers are big-endian. A string or a frontier is a `u32` length and that many bytes.
//! A list is a `u32` count and that many items. An optional value is a flag byte, 0 or 1, and
//! the value when the flag is 1.
//!
//! ```text
//! format version (1)
//! entry
//!     range_version      u8
//!     start_height       u32
//!     end_height         u32
//!     end_hash           32 bytes
//!     end_tree_sizes     sapling u32, orchard u32, ironwood u32
//!     frontiers          sapling, orchard, ironwood: bytes
//!     subtree_roots      list of (index u16, pool u8, root 32 bytes in the pool's byte order)
//!     blob_digest        32 bytes
//!     blob_len           u64
//!     epoch_end          u8
//! state                  u8
//! verified_by            optional (url string, version string)
//! verified_at            optional u64
//! attested               optional (end_hash 32 bytes, three optional 32-byte roots,
//!                        subtree_roots list)
//! sampled_heights        list of u32
//! ```

use heed::{
    Database, RoTxn, RwTxn,
    byteorder::BigEndian,
    types::{Bytes, U32},
};
use sapling_crypto::Node as SaplingNode;
use zakura_chain::{block, ironwood, orchard, sapling, subtree::NoteCommitmentSubtreeIndex};

use super::IndexError;
use crate::codec::{CodecError, Reader, TreeSizes};

mod wire;
pub use wire::WireError;

const FORMAT_VERSION: u8 = 1;
const SAPLING_TAG: u8 = 0;
const ORCHARD_TAG: u8 = 1;
const IRONWOOD_TAG: u8 = 2;

/// The BLAKE3 digest of a range blob.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlobDigest(pub [u8; 32]);

impl BlobDigest {
    /// Calculates the BLAKE3 digest of `blob`.
    pub fn hash(blob: &[u8]) -> Self {
        Self(*blake3::hash(blob).as_bytes())
    }
}

/// The root node of a completed note commitment subtree, in its pool's node type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubtreeNode {
    /// A Sapling subtree root.
    Sapling(SaplingNode),
    /// An Orchard subtree root.
    Orchard(orchard::tree::Node),
    /// An Ironwood subtree root.
    Ironwood(ironwood::tree::Node),
}

impl SubtreeNode {
    fn encode_into(self, bytes: &mut Vec<u8>) {
        let (tag, node) = match self {
            Self::Sapling(node) => (SAPLING_TAG, node.to_bytes()),
            Self::Orchard(node) => (ORCHARD_TAG, node.to_repr()),
            Self::Ironwood(node) => (IRONWOOD_TAG, node.to_repr()),
        };
        bytes.push(tag);
        bytes.extend_from_slice(&node);
    }

    fn decode_from(reader: &mut Reader<'_>) -> Result<Self, CodecError> {
        let tag = reader.u8()?;
        let bytes: [u8; 32] = reader.array()?;
        match tag {
            SAPLING_TAG => Option::from(SaplingNode::from_bytes(bytes)).map(Self::Sapling),
            ORCHARD_TAG => orchard::tree::Node::try_from(bytes).ok().map(Self::Orchard),
            IRONWOOD_TAG => ironwood::tree::Node::try_from(bytes).ok().map(Self::Ironwood),
            _ => None,
        }
        .ok_or(CodecError::InvalidRecord)
    }
}

/// How far a sealed range has been proven.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VerifyState {
    /// The range is installed, and no check has run.
    Unverified,
    /// The digest, the hash chain, and the tree sizes check out, and the end hash matches the
    /// node. The commitment trees have not been rebuilt.
    ChainOk,
    /// The rebuilt tree roots match the node's answers.
    Verified,
    /// A check failed. The range must not be served.
    Rejected,
}

impl VerifyState {
    const fn tag(self) -> u8 {
        match self {
            Self::Unverified => 0,
            Self::ChainOk => 1,
            Self::Verified => 2,
            Self::Rejected => 3,
        }
    }

    const fn from_tag(tag: u8) -> Result<Self, CodecError> {
        match tag {
            0 => Ok(Self::Unverified),
            1 => Ok(Self::ChainOk),
            2 => Ok(Self::Verified),
            3 => Ok(Self::Rejected),
            _ => Err(CodecError::InvalidRecord),
        }
    }
}

/// The root of one completed note commitment subtree.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubtreeRoot {
    /// The subtree's index in its pool's tree.
    pub index: NoteCommitmentSubtreeIndex,
    /// The subtree root. Its variant names the pool.
    pub root: SubtreeNode,
}

/// The three note commitment tree frontiers at the end of a range, each in the `zcashd` RPC
/// encoding. An empty frontier means the pool was not active at that height.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Frontiers {
    /// Sapling frontier.
    pub sapling: Vec<u8>,
    /// Orchard frontier.
    pub orchard: Vec<u8>,
    /// Ironwood frontier.
    pub ironwood: Vec<u8>,
}

impl Frontiers {
    /// Returns `true` if each pool that is active in `before` is also active in `self`.
    ///
    /// An empty frontier means that the pool is not active: `z_gettreestate` gives no tree for
    /// it. An active pool always has a frontier, also when its tree is empty. A pool does not
    /// become inactive, so `false` means that `self` is wrong.
    pub const fn follows(&self, before: &Self) -> bool {
        (before.sapling.is_empty() || !self.sapling.is_empty())
            && (before.orchard.is_empty() || !self.orchard.is_empty())
            && (before.ironwood.is_empty() || !self.ironwood.is_empty())
    }
}

/// What a snapshot manifest says about one sealed range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestEntry {
    /// The range blob format version.
    pub range_version: u8,
    /// The first height of the range.
    pub start_height: u32,
    /// The last height of the range.
    pub end_height: u32,
    /// The hash of the block at `end_height`.
    pub end_hash: block::Hash,
    /// The cumulative tree sizes after the block at `end_height`.
    pub end_tree_sizes: TreeSizes,
    /// The tree frontiers after the block at `end_height`.
    pub frontiers: Frontiers,
    /// The subtrees that the range completed.
    pub subtree_roots: Vec<SubtreeRoot>,
    /// The BLAKE3 digest of the range blob.
    pub blob_digest: BlobDigest,
    /// The length of the range blob in bytes.
    pub blob_len: u64,
    /// Whether the range closes a verification epoch.
    pub epoch_end: bool,
}

/// A manifest entry and how far the index that serves it has proven the range. The Replicator
/// service sends one for each range that it lists.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExportedRange {
    /// The manifest entry for the range.
    pub entry: ManifestEntry,
    /// How far the serving index has proven the range.
    pub state: VerifyState,
}

/// The node that verified a range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeId {
    /// The node's RPC URL.
    pub url: String,
    /// The node's version string.
    pub version: String,
}

/// The tree roots a node reported for one height. A root is `None` when the pool was not active
/// at that height.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PoolRoots {
    /// Sapling root.
    pub sapling: Option<sapling::tree::Root>,
    /// Orchard root.
    pub orchard: Option<orchard::tree::Root>,
    /// Ironwood root.
    pub ironwood: Option<ironwood::tree::Root>,
}

/// A node's answers for the end of a range, kept so that the node is asked once.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Attested {
    /// The hash the node reports for the range's end height.
    pub end_hash: block::Hash,
    /// The tree roots the node reports for the range's end height.
    pub roots: PoolRoots,
    /// The subtree roots the node reports for the subtrees the range completed.
    pub subtree_roots: Vec<SubtreeRoot>,
}

/// Everything the index keeps about one sealed range besides its blocks.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangeMeta {
    /// The manifest entry for the range.
    pub entry: ManifestEntry,
    /// How far the range has been proven.
    pub state: VerifyState,
    /// The node that verified the range, once it is [`VerifyState::Verified`].
    pub verified_by: Option<NodeId>,
    /// When the range was verified, in Unix seconds.
    pub verified_at: Option<u64>,
    /// The node's cached answers.
    pub attested: Option<Attested>,
    /// The heights whose full blocks were byte-compared with the node. Empty below the node's
    /// retention window.
    pub sampled_heights: Vec<u32>,
}

impl RangeMeta {
    /// Encodes the record in the layout the module docs give.
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = vec![FORMAT_VERSION];
        self.entry.encode_into(&mut bytes);
        bytes.push(self.state.tag());
        put_option(&mut bytes, self.verified_by.as_ref(), |bytes, node| {
            put_bytes(bytes, node.url.as_bytes());
            put_bytes(bytes, node.version.as_bytes());
        });
        put_option(&mut bytes, self.verified_at, |bytes, at| {
            bytes.extend_from_slice(&at.to_be_bytes());
        });
        put_option(&mut bytes, self.attested.as_ref(), |bytes, attested| {
            bytes.extend_from_slice(&attested.end_hash.0);
            put_option(bytes, attested.roots.sapling, |bytes, root| {
                bytes.extend_from_slice(&<[u8; 32]>::from(root));
            });
            put_option(bytes, attested.roots.orchard, |bytes, root| {
                bytes.extend_from_slice(&<[u8; 32]>::from(root));
            });
            put_option(bytes, attested.roots.ironwood, |bytes, root| {
                bytes.extend_from_slice(&<[u8; 32]>::from(root));
            });
            put_subtree_roots(bytes, &attested.subtree_roots);
        });
        put_count(&mut bytes, self.sampled_heights.len());
        for height in &self.sampled_heights {
            bytes.extend_from_slice(&height.to_be_bytes());
        }
        bytes
    }

    /// Decodes a record written by [`RangeMeta::encode`].
    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut reader = Reader::new(bytes);
        let version = reader.u8()?;
        if version != FORMAT_VERSION {
            return Err(CodecError::Version { kind: "range meta", version });
        }
        let entry = ManifestEntry::decode_from(&mut reader)?;
        let state = VerifyState::from_tag(reader.u8()?)?;
        let verified_by = take_option(&mut reader, |reader| {
            Ok(NodeId { url: take_string(reader)?, version: take_string(reader)? })
        })?;
        let verified_at = take_option(&mut reader, Reader::u64)?;
        let attested = take_option(&mut reader, |reader| {
            let end_hash = block::Hash(reader.array()?);
            let sapling = take_option(reader, take_root::<sapling::tree::Root>)?;
            let orchard = take_option(reader, take_root::<orchard::tree::Root>)?;
            let ironwood = take_option(reader, take_root::<ironwood::tree::Root>)?;
            Ok(Attested {
                end_hash,
                roots: PoolRoots { sapling, orchard, ironwood },
                subtree_roots: take_subtree_roots(reader)?,
            })
        })?;
        let count = reader.len()?;
        let sampled_heights = (0..count).map(|_| reader.u32()).collect::<Result<Vec<_>, _>>()?;
        reader.finish()?;
        Ok(Self { entry, state, verified_by, verified_at, attested, sampled_heights })
    }
}

impl ManifestEntry {
    fn encode_into(&self, bytes: &mut Vec<u8>) {
        bytes.push(self.range_version);
        bytes.extend_from_slice(&self.start_height.to_be_bytes());
        bytes.extend_from_slice(&self.end_height.to_be_bytes());
        bytes.extend_from_slice(&self.end_hash.0);
        for size in
            [self.end_tree_sizes.sapling, self.end_tree_sizes.orchard, self.end_tree_sizes.ironwood]
        {
            bytes.extend_from_slice(&size.to_be_bytes());
        }
        for frontier in [&self.frontiers.sapling, &self.frontiers.orchard, &self.frontiers.ironwood]
        {
            put_bytes(bytes, frontier);
        }
        put_subtree_roots(bytes, &self.subtree_roots);
        bytes.extend_from_slice(&self.blob_digest.0);
        bytes.extend_from_slice(&self.blob_len.to_be_bytes());
        bytes.push(u8::from(self.epoch_end));
    }

    fn decode_from(reader: &mut Reader<'_>) -> Result<Self, CodecError> {
        let range_version = reader.u8()?;
        let start_height = reader.u32()?;
        let end_height = reader.u32()?;
        let end_hash = block::Hash(reader.array()?);
        let end_tree_sizes =
            TreeSizes { sapling: reader.u32()?, orchard: reader.u32()?, ironwood: reader.u32()? };
        let frontiers = Frontiers {
            sapling: reader.bytes()?.to_vec(),
            orchard: reader.bytes()?.to_vec(),
            ironwood: reader.bytes()?.to_vec(),
        };
        let subtree_roots = take_subtree_roots(reader)?;
        let blob_digest = BlobDigest(reader.array()?);
        let blob_len = reader.u64()?;
        let epoch_end = take_bool(reader)?;
        Ok(Self {
            range_version,
            start_height,
            end_height,
            end_hash,
            end_tree_sizes,
            frontiers,
            subtree_roots,
            blob_digest,
            blob_len,
            epoch_end,
        })
    }
}

fn put_count(bytes: &mut Vec<u8>, count: usize) {
    let count = u32::try_from(count).expect("range metadata lists are far below u32::MAX items");
    bytes.extend_from_slice(&count.to_be_bytes());
}

fn put_bytes(bytes: &mut Vec<u8>, value: &[u8]) {
    put_count(bytes, value.len());
    bytes.extend_from_slice(value);
}

fn put_option<T>(bytes: &mut Vec<u8>, value: Option<T>, put: impl FnOnce(&mut Vec<u8>, T)) {
    match value {
        Some(value) => {
            bytes.push(1);
            put(bytes, value);
        }
        None => bytes.push(0),
    }
}

fn put_subtree_roots(bytes: &mut Vec<u8>, roots: &[SubtreeRoot]) {
    put_count(bytes, roots.len());
    for root in roots {
        bytes.extend_from_slice(&root.index.0.to_be_bytes());
        root.root.encode_into(bytes);
    }
}

fn take_bool(reader: &mut Reader<'_>) -> Result<bool, CodecError> {
    match reader.u8()? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(CodecError::InvalidRecord),
    }
}

fn take_option<'a, T>(
    reader: &mut Reader<'a>,
    take: impl FnOnce(&mut Reader<'a>) -> Result<T, CodecError>,
) -> Result<Option<T>, CodecError> {
    if take_bool(reader)? { take(reader).map(Some) } else { Ok(None) }
}

fn take_string(reader: &mut Reader<'_>) -> Result<String, CodecError> {
    String::from_utf8(reader.bytes()?.to_vec()).map_err(|_| CodecError::InvalidRecord)
}

/// Reads a 32-byte tree root and rejects bytes that are not a field element of the pool.
fn take_root<R: TryFrom<[u8; 32]>>(reader: &mut Reader<'_>) -> Result<R, CodecError> {
    R::try_from(reader.array()?).map_err(|_| CodecError::InvalidRecord)
}

fn take_subtree_roots(reader: &mut Reader<'_>) -> Result<Vec<SubtreeRoot>, CodecError> {
    let count = reader.len()?;
    (0..count)
        .map(|_| {
            Ok(SubtreeRoot {
                index: NoteCommitmentSubtreeIndex(reader.u16()?),
                root: SubtreeNode::decode_from(reader)?,
            })
        })
        .collect()
}

/// The `range_meta` table.
///
/// - Key: the first height of a sealed range, `u32` big-endian.
/// - Value: a [`RangeMeta`] record, encoded as the module docs give.
///
/// A key is always the start of a range in `sealed_ranges`. The table has no row for a range
/// that nothing has described yet.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RangeMetaTable(Database<U32<BigEndian>, Bytes>);

impl RangeMetaTable {
    pub(crate) const fn new(db: Database<U32<BigEndian>, Bytes>) -> Self {
        Self(db)
    }

    pub(crate) fn get(&self, txn: &RoTxn<'_>, start: u32) -> Result<Option<RangeMeta>, IndexError> {
        self.0.get(txn, &start)?.map(RangeMeta::decode).transpose().map_err(Into::into)
    }

    pub(crate) fn put(
        &self,
        txn: &mut RwTxn<'_>,
        start: u32,
        meta: &RangeMeta,
    ) -> Result<(), IndexError> {
        Ok(self.0.put(txn, &start, meta.encode().as_slice())?)
    }

    /// Deletes the rows of every range that starts at or above `start`.
    pub(crate) fn delete_from(&self, txn: &mut RwTxn<'_>, start: u32) -> Result<(), IndexError> {
        self.0.delete_range(txn, &(start..))?;
        Ok(())
    }

    /// The rows of every range that starts at or above `start`, in height order.
    pub(crate) fn rows_from(
        &self,
        txn: &RoTxn<'_>,
        start: u32,
    ) -> Result<Vec<RangeMeta>, IndexError> {
        self.0.range(txn, &(start..))?.map(|entry| Ok(RangeMeta::decode(entry?.1)?)).collect()
    }

    /// The start heights of every row, in height order.
    pub(crate) fn starts(&self, txn: &RoTxn<'_>) -> Result<Vec<u32>, IndexError> {
        self.0.iter(txn)?.map(|entry| Ok(entry?.0)).collect()
    }
}

/// A record for the range at `start` with every optional part present and every list non-empty.
#[cfg(test)]
pub(crate) fn full_meta(start: u32) -> RangeMeta {
    use crate::test_utils::hash;

    RangeMeta {
        entry: ManifestEntry {
            range_version: 1,
            start_height: start,
            end_height: start + 999,
            end_hash: hash(start + 999),
            end_tree_sizes: TreeSizes { sapling: 61_231_877, orchard: 12, ironwood: 0 },
            frontiers: Frontiers {
                sapling: vec![0xab; 1_100],
                orchard: vec![0xcd; 900],
                ironwood: Vec::new(),
            },
            subtree_roots: vec![
                SubtreeRoot {
                    index: NoteCommitmentSubtreeIndex(934),
                    root: SubtreeNode::Sapling(SaplingNode::from_bytes([1; 32]).unwrap()),
                },
                SubtreeRoot {
                    index: NoteCommitmentSubtreeIndex(0),
                    root: SubtreeNode::Orchard(orchard::tree::Node::try_from([2; 32]).unwrap()),
                },
            ],
            blob_digest: BlobDigest([7; 32]),
            blob_len: 121_034_512,
            epoch_end: true,
        },
        state: VerifyState::Verified,
        verified_by: Some(NodeId {
            url: "https://node.example:8232".to_owned(),
            version: "zakurad 1.5.1".to_owned(),
        }),
        verified_at: Some(1_792_000_000),
        attested: Some(Attested {
            end_hash: hash(start + 999),
            roots: PoolRoots {
                sapling: Some(sapling::tree::Root::try_from([3; 32]).unwrap()),
                orchard: Some(orchard::tree::Root::try_from([4; 32]).unwrap()),
                ironwood: None,
            },
            subtree_roots: vec![SubtreeRoot {
                index: NoteCommitmentSubtreeIndex(5),
                root: SubtreeNode::Ironwood(ironwood::tree::Node::try_from([6; 32]).unwrap()),
            }],
        }),
        sampled_heights: vec![start + 17, start + 503],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::hash;

    /// A record with every optional part absent and every list empty.
    fn bare_meta(start: u32) -> RangeMeta {
        RangeMeta {
            entry: ManifestEntry {
                range_version: 1,
                start_height: start,
                end_height: start + 999,
                end_hash: hash(start + 999),
                end_tree_sizes: TreeSizes::default(),
                frontiers: Frontiers::default(),
                subtree_roots: Vec::new(),
                blob_digest: BlobDigest([0; 32]),
                blob_len: 0,
                epoch_end: false,
            },
            state: VerifyState::Unverified,
            verified_by: None,
            verified_at: None,
            attested: None,
            sampled_heights: Vec::new(),
        }
    }

    #[test]
    fn full_and_bare_records_round_trip() {
        for meta in [full_meta(1_785_000), bare_meta(0)] {
            let bytes = meta.encode();
            assert_eq!(bytes[0], FORMAT_VERSION);
            assert_eq!(RangeMeta::decode(&bytes).unwrap(), meta);
        }
    }

    #[test]
    fn every_state_round_trips() {
        for state in [
            VerifyState::Unverified,
            VerifyState::ChainOk,
            VerifyState::Verified,
            VerifyState::Rejected,
        ] {
            assert_eq!(VerifyState::from_tag(state.tag()).unwrap(), state);
        }
        assert_eq!(VerifyState::from_tag(4), Err(CodecError::InvalidRecord));
    }

    /// A subtree root is a pool tag and 32 bytes. The decoder refuses an unknown tag and bytes
    /// that are not a field element of the pool.
    #[test]
    fn subtree_nodes_round_trip_and_reject_bad_bytes() {
        let nodes = [
            SubtreeNode::Sapling(SaplingNode::from_bytes([1; 32]).unwrap()),
            SubtreeNode::Orchard(orchard::tree::Node::try_from([2; 32]).unwrap()),
            SubtreeNode::Ironwood(ironwood::tree::Node::try_from([3; 32]).unwrap()),
        ];
        for (tag, node) in nodes.into_iter().enumerate() {
            let mut bytes = Vec::new();
            node.encode_into(&mut bytes);
            assert_eq!(bytes[0], tag as u8);
            assert_eq!(bytes.len(), 33);
            assert_eq!(SubtreeNode::decode_from(&mut Reader::new(&bytes)).unwrap(), node);

            let mut unknown_tag = bytes.clone();
            unknown_tag[0] = 3;
            assert_eq!(
                SubtreeNode::decode_from(&mut Reader::new(&unknown_tag)),
                Err(CodecError::InvalidRecord)
            );
            // All ones is above every pool's field modulus.
            let not_a_field_element: Vec<u8> = [bytes[0]].into_iter().chain([0xff; 32]).collect();
            assert_eq!(
                SubtreeNode::decode_from(&mut Reader::new(&not_a_field_element)),
                Err(CodecError::InvalidRecord)
            );
        }
    }

    /// An attested root that is not a field element is refused.
    #[test]
    fn decode_rejects_an_attested_root_that_is_not_a_field_element() {
        let meta = full_meta(1_000);
        let mut bytes = meta.encode();
        let sapling_root = <[u8; 32]>::from(meta.attested.unwrap().roots.sapling.unwrap());
        let at = bytes.windows(32).position(|window| window == sapling_root).unwrap();
        bytes[at..at + 32].copy_from_slice(&[0xff; 32]);
        assert_eq!(RangeMeta::decode(&bytes), Err(CodecError::InvalidRecord));
    }

    #[test]
    fn decode_rejects_damaged_records() {
        let meta = full_meta(2_000);
        let bytes = meta.encode();

        let mut other_version = bytes.clone();
        other_version[0] = 2;
        assert_eq!(
            RangeMeta::decode(&other_version),
            Err(CodecError::Version { kind: "range meta", version: 2 })
        );

        assert_eq!(RangeMeta::decode(&bytes[..bytes.len() - 1]), Err(CodecError::Truncated));

        let mut trailing = bytes.clone();
        trailing.push(0);
        assert_eq!(RangeMeta::decode(&trailing), Err(CodecError::TrailingBytes));

        // The state byte follows the entry; an unknown state is refused.
        let entry_len = {
            let mut entry = Vec::new();
            meta.entry.encode_into(&mut entry);
            entry.len()
        };
        let mut bad_state = bytes.clone();
        bad_state[1 + entry_len] = 9;
        assert_eq!(RangeMeta::decode(&bad_state), Err(CodecError::InvalidRecord));

        // The epoch_end flag is the last byte of the entry; a flag above 1 is refused.
        let mut bad_flag = bytes;
        bad_flag[entry_len] = 2;
        assert_eq!(RangeMeta::decode(&bad_flag), Err(CodecError::InvalidRecord));
    }

    #[test]
    fn a_pool_that_becomes_inactive_does_not_follow() {
        let active = Frontiers { sapling: vec![1], orchard: vec![0, 0, 0], ironwood: Vec::new() };
        let mut later = Frontiers { ironwood: vec![2], ..active.clone() };
        assert!(later.follows(&active));
        assert!(active.follows(&Frontiers::default()));

        later.orchard.clear();
        assert!(!later.follows(&active));
    }

    #[test]
    fn decode_rejects_a_url_that_is_not_utf8() {
        let mut meta = bare_meta(0);
        meta.verified_by = Some(NodeId { url: "ok".to_owned(), version: String::new() });
        let mut bytes = meta.encode();
        let url_start = bytes.windows(2).position(|window| window == b"ok").unwrap();
        bytes[url_start] = 0xff;
        assert_eq!(RangeMeta::decode(&bytes), Err(CodecError::InvalidRecord));
    }
}
