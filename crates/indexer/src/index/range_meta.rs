//! The `range_meta` table: one [`RangeMeta`] record per sealed range.
//!
//! A record is what a replica needs to know about a range without decoding its blocks: the
//! manifest entry a snapshot publishes for it, how far the range has been verified against a
//! node, and the node's answers so that they are never asked for twice. The importer writes the
//! rows of the ranges that it installs. The `range_meta` filler of `ztreamer-service` writes the
//! rows of the ranges that the index seals from node blocks, in both modes.
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
use zakura_chain::{block, ironwood, orchard, sapling};

use super::IndexError;
use crate::{
    codec::{CodecError, Reader, put_bytes, put_count, take_bool},
    manifest_entry::ManifestEntry,
    pool::{PoolRoots, SubtreeRoot, put_subtree_roots, take_subtree_roots},
};

const FORMAT_VERSION: u8 = 1;

/// How far a sealed range has been proven.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VerifyState {
    /// The range is installed, and no check has run.
    Unverified,
    /// The rebuilt tree roots match the node's answers.
    Verified,
    /// A check failed. The range must not be served.
    Rejected,
}

impl VerifyState {
    /// The stored tag of the state. Tag 1 was `ChainOk`. It is not used, and a decode refuses
    /// it.
    const fn tag(self) -> u8 {
        match self {
            Self::Unverified => 0,
            Self::Verified => 2,
            Self::Rejected => 3,
        }
    }

    const fn from_tag(tag: u8) -> Result<Self, CodecError> {
        match tag {
            0 => Ok(Self::Unverified),
            2 => Ok(Self::Verified),
            3 => Ok(Self::Rejected),
            _ => Err(CodecError::InvalidRecord),
        }
    }
}

/// The node that verified a range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeId {
    /// The node's RPC URL.
    pub url: String,
    /// The node's version string.
    pub version: String,
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

fn put_option<T>(bytes: &mut Vec<u8>, value: Option<T>, put: impl FnOnce(&mut Vec<u8>, T)) {
    match value {
        Some(value) => {
            bytes.push(1);
            put(bytes, value);
        }
        None => bytes.push(0),
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
    use sapling_crypto::Node as SaplingNode;
    use zakura_chain::subtree::NoteCommitmentSubtreeIndex;

    use crate::{
        codec::TreeSizes,
        manifest_entry::{BlobDigest, Frontiers},
        pool::SubtreeNode,
        test_utils::hash,
    };

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
    use crate::{
        codec::TreeSizes,
        manifest_entry::{BlobDigest, Frontiers},
        test_utils::hash,
    };

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
        for state in [VerifyState::Unverified, VerifyState::Verified, VerifyState::Rejected] {
            assert_eq!(VerifyState::from_tag(state.tag()).unwrap(), state);
        }
        // Tag 1 was `ChainOk`.
        for tag in [1, 4] {
            assert_eq!(VerifyState::from_tag(tag), Err(CodecError::InvalidRecord));
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
    fn decode_rejects_a_url_that_is_not_utf8() {
        let mut meta = bare_meta(0);
        meta.verified_by = Some(NodeId { url: "ok".to_owned(), version: String::new() });
        let mut bytes = meta.encode();
        let url_start = bytes.windows(2).position(|window| window == b"ok").unwrap();
        bytes[url_start] = 0xff;
        assert_eq!(RangeMeta::decode(&bytes), Err(CodecError::InvalidRecord));
    }
}
