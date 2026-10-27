//! The snapshot manifest: the JSON document that lists every sealed range of a snapshot.
//!
//! ```json
//! {
//!   "network": "Mainnet",
//!   "genesis_hash": "00040fe8ec8471911baa1db1266ea15dd06b4a8a5c453883c000b031973dce08",
//!   "sealed_through": 1785999,
//!   "manifest_version": 1,
//!   "entries": [{
//!     "range_version": 1,
//!     "start_height": 1785000, "end_height": 1785999,
//!     "end_hash": "<32 bytes hex, display order>",
//!     "end_tree_sizes": { "sapling": 61231877, "orchard": 0, "ironwood": 0 },
//!     "sapling_frontier": "<hex>", "orchard_frontier": "<hex>", "ironwood_frontier": "<hex>",
//!     "subtree_roots": [{ "pool": "sapling", "index": 934, "root": "<32 bytes hex>" }],
//!     "blob_digest": "<BLAKE3 hex>", "blob_len": 121034512,
//!     "epoch_end": true
//!   }]
//! }
//! ```
//!
//! Block hashes are hex in display order, as the node prints them. A frontier is the hex of the
//! `zcashd` RPC tree encoding. An empty string means that the pool is not active; an active pool
//! with an empty tree has the encoding of the empty tree. A subtree root is the hex of the pool's
//! byte order, as `z_getsubtreesbyindex` prints it.
//!
//! The entries convert to [`ManifestEntry`], so that the indexer stays free of serde.

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use serde_with::{DeserializeAs, DisplayFromStr, SerializeAs, hex::Hex, serde_as};
use zakura_chain::{block, ironwood, orchard, subtree::NoteCommitmentSubtreeIndex};
use ztreamer_indexer::{
    codec::TreeSizes,
    index::{
        RANGE_SIZE,
        range_meta::{BlobDigest, Frontiers, ManifestEntry, SubtreeNode, SubtreeRoot},
    },
};

use crate::epoch::Epochs;

/// The manifest format version that this crate reads.
pub const MANIFEST_VERSION: u32 = 1;

/// A snapshot manifest that obeys the rules in the module docs.
///
/// The fields are private. Each way to get a manifest checks the rules: [`Manifest::new`],
/// [`Manifest::from_json`] and serde. Thus a `Manifest` is always valid.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(try_from = "ManifestFields")]
pub struct Manifest(ManifestFields);

/// The fields of a manifest, before the rules are checked.
#[serde_as]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ManifestFields {
    network: String,
    #[serde_as(as = "DisplayFromStr")]
    genesis_hash: block::Hash,
    sealed_through: u32,
    manifest_version: u32,
    #[serde_as(as = "Vec<EntryJson>")]
    entries: Vec<ManifestEntry>,
}

/// A manifest that does not parse or does not obey the rules in the module docs.
#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    /// The JSON does not parse into a manifest.
    #[error("manifest JSON is not valid")]
    Json(#[from] serde_json::Error),
    /// The manifest format version is not [`MANIFEST_VERSION`].
    #[error("unsupported manifest version {version}")]
    Version {
        /// The version in the manifest.
        version: u32,
    },
    /// `sealed_through` is not the last height of the last entry.
    #[error("sealed_through {sealed_through} is not the end of the last entry ({last_end:?})")]
    SealedThrough {
        /// The `sealed_through` of the manifest.
        sealed_through: u32,
        /// The last height of the last entry, or `None` when the manifest has no entries.
        last_end: Option<u32>,
    },
    /// The entry does not start one height after the previous entry, or at genesis.
    #[error("entry {index} starts at {start_height}, not after the previous entry")]
    Discontiguous {
        /// The position of the entry in the manifest.
        index: usize,
        /// The start height of the entry.
        start_height: u32,
    },
    /// The entry is not one aligned range.
    #[error("entry {index} does not hold {RANGE_SIZE} blocks")]
    RangeLength {
        /// The position of the entry in the manifest.
        index: usize,
    },
    /// A tree size of the entry is smaller than in the previous entry.
    #[error("entry {index} has a smaller tree than the previous entry")]
    TreeSizes {
        /// The position of the entry in the manifest.
        index: usize,
    },
    /// A frontier of the entry is empty, but the same frontier of the previous entry is not. An
    /// empty frontier means that the pool is not active, and a pool does not become inactive.
    #[error("entry {index} has an empty frontier for a pool that is active")]
    PoolInactive {
        /// The position of the entry in the manifest.
        index: usize,
    },
    /// The `epoch_end` flag of the entry does not agree with the epoch rule.
    #[error("entry {index} has the wrong epoch_end flag; the epoch rule gives {expected}")]
    EpochEnd {
        /// The position of the entry in the manifest.
        index: usize,
        /// The flag that the epoch rule gives.
        expected: bool,
    },
}

impl Manifest {
    /// The manifest of `entries`, one per sealed range in height order from genesis, for
    /// `network` with the genesis block hash. It is at [`MANIFEST_VERSION`], with
    /// `sealed_through` at the end of the last entry, and it obeys the rules.
    ///
    /// ```
    /// use zakura_chain::block;
    /// use ztreamer_indexer::index::range_meta::ManifestEntry;
    /// use ztreamer_verify::manifest::{Manifest, ManifestError};
    ///
    /// fn manifest(genesis: block::Hash, entries: Vec<ManifestEntry>) -> Result<Manifest, ManifestError> {
    ///     Manifest::new("Mainnet", genesis, entries)
    /// }
    /// ```
    pub fn new(
        network: impl Into<String>,
        genesis_hash: block::Hash,
        entries: Vec<ManifestEntry>,
    ) -> Result<Self, ManifestError> {
        ManifestFields {
            network: network.into(),
            genesis_hash,
            sealed_through: entries.last().map_or(0, |entry| entry.end_height),
            manifest_version: MANIFEST_VERSION,
            entries,
        }
        .try_into()
    }

    /// Parses a manifest and checks the rules.
    pub fn from_json(json: &str) -> Result<Self, ManifestError> {
        serde_json::from_str::<ManifestFields>(json)?.try_into()
    }

    /// The network name, for example `Mainnet`.
    pub fn network(&self) -> &str {
        &self.0.network
    }

    /// The hash of the genesis block.
    pub const fn genesis_hash(&self) -> block::Hash {
        self.0.genesis_hash
    }

    /// The last height of the last sealed range.
    pub const fn sealed_through(&self) -> u32 {
        self.0.sealed_through
    }

    /// One entry per sealed range, in height order from genesis.
    pub fn entries(&self) -> &[ManifestEntry] {
        &self.0.entries
    }

    /// The verification epochs of the manifest.
    pub fn epochs(&self) -> Epochs<'_> {
        Epochs::new(&self.0.entries)
    }
}

impl Serialize for Manifest {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl TryFrom<ManifestFields> for Manifest {
    type Error = ManifestError;

    /// Checks that:
    ///
    /// - the version is [`MANIFEST_VERSION`];
    /// - the entries are one aligned range each, contiguous from genesis, with tree sizes that
    ///   do not decrease, and with no pool that becomes inactive;
    /// - `sealed_through` is the last height of the last entry;
    /// - `epoch_end` agrees with [`Epochs`].
    fn try_from(fields: ManifestFields) -> Result<Self, ManifestError> {
        if fields.manifest_version != MANIFEST_VERSION {
            return Err(ManifestError::Version { version: fields.manifest_version });
        }
        fields
            .entries
            .iter()
            .enumerate()
            .try_for_each(|(index, entry)| fields.check_entry(index, entry))?;
        let last_end = fields.entries.last().map(|entry| entry.end_height);
        if last_end != Some(fields.sealed_through) {
            return Err(ManifestError::SealedThrough {
                sealed_through: fields.sealed_through,
                last_end,
            });
        }
        fields.check_epoch_ends()?;
        Ok(Self(fields))
    }
}

impl ManifestFields {
    /// Checks that entry `index` is one aligned range, and that it follows the entry before it
    /// or starts at genesis.
    fn check_entry(&self, index: usize, entry: &ManifestEntry) -> Result<(), ManifestError> {
        let previous = index.checked_sub(1).map(|previous| &self.entries[previous]);
        let expected_start =
            previous.map_or(Some(0), |previous| previous.end_height.checked_add(1));
        if expected_start != Some(entry.start_height) {
            return Err(ManifestError::Discontiguous { index, start_height: entry.start_height });
        }
        if entry.start_height.checked_add(RANGE_SIZE - 1) != Some(entry.end_height) {
            return Err(ManifestError::RangeLength { index });
        }
        let Some(previous) = previous else {
            return Ok(());
        };
        if !entry.end_tree_sizes.follows(previous.end_tree_sizes) {
            return Err(ManifestError::TreeSizes { index });
        }
        if !entry.frontiers.follows(&previous.frontiers) {
            return Err(ManifestError::PoolInactive { index });
        }
        Ok(())
    }

    /// Checks that each entry has `epoch_end` set when it is the last entry of its epoch.
    fn check_epoch_ends(&self) -> Result<(), ManifestError> {
        Epochs::new(&self.entries)
            .flat_map(|epoch| {
                let last = epoch.entries().len() - 1;
                epoch.entries().iter().enumerate().map(move |(at, entry)| (entry, at == last))
            })
            .enumerate()
            .find(|(_, (entry, expected))| entry.epoch_end != *expected)
            .map_or(Ok(()), |(index, (_, expected))| {
                Err(ManifestError::EpochEnd { index, expected })
            })
    }
}

/// The JSON form of a [`ManifestEntry`].
#[serde_as]
#[derive(Deserialize, Serialize)]
struct EntryJson {
    range_version: u8,
    start_height: u32,
    end_height: u32,
    #[serde_as(as = "DisplayFromStr")]
    end_hash: block::Hash,
    #[serde(with = "TreeSizesJson")]
    end_tree_sizes: TreeSizes,
    #[serde_as(as = "Hex")]
    sapling_frontier: Vec<u8>,
    #[serde_as(as = "Hex")]
    orchard_frontier: Vec<u8>,
    #[serde_as(as = "Hex")]
    ironwood_frontier: Vec<u8>,
    subtree_roots: Vec<SubtreeRootJson>,
    #[serde_as(as = "Hex")]
    blob_digest: [u8; 32],
    blob_len: u64,
    epoch_end: bool,
}

/// The JSON form of [`TreeSizes`].
#[derive(Deserialize, Serialize)]
#[serde(remote = "TreeSizes")]
struct TreeSizesJson {
    sapling: u32,
    orchard: u32,
    ironwood: u32,
}

/// A note commitment pool.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Pool {
    /// The Sapling pool.
    Sapling,
    /// The Orchard pool.
    Orchard,
    /// The Ironwood pool.
    Ironwood,
}

impl Pool {
    /// The three pools.
    pub const ALL: [Self; 3] = [Self::Sapling, Self::Orchard, Self::Ironwood];

    /// The name of the pool in the manifest and in `z_getsubtreesbyindex`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sapling => "sapling",
            Self::Orchard => "orchard",
            Self::Ironwood => "ironwood",
        }
    }

    /// Reads a subtree root of this pool from `bytes` in the byte order of the pool, as
    /// `z_getsubtreesbyindex` prints it. Returns `None` when the bytes are not a node of the pool.
    pub fn subtree_node(self, bytes: [u8; 32]) -> Option<SubtreeNode> {
        match self {
            Self::Sapling => {
                Option::from(sapling_crypto::Node::from_bytes(bytes)).map(SubtreeNode::Sapling)
            }
            Self::Orchard => orchard::tree::Node::try_from(bytes).ok().map(SubtreeNode::Orchard),
            Self::Ironwood => ironwood::tree::Node::try_from(bytes).ok().map(SubtreeNode::Ironwood),
        }
    }
}

impl From<&SubtreeNode> for Pool {
    /// The pool of a subtree root.
    fn from(node: &SubtreeNode) -> Self {
        match node {
            SubtreeNode::Sapling(_) => Self::Sapling,
            SubtreeNode::Orchard(_) => Self::Orchard,
            SubtreeNode::Ironwood(_) => Self::Ironwood,
        }
    }
}

/// The JSON form of a [`SubtreeRoot`].
#[serde_as]
#[derive(Deserialize, Serialize)]
struct SubtreeRootJson {
    pool: Pool,
    index: u16,
    #[serde_as(as = "Hex")]
    root: [u8; 32],
}

/// The root bytes are not a field element of the pool.
#[derive(Debug, thiserror::Error)]
#[error("{pool:?} subtree root {index} is not a field element")]
struct InvalidSubtreeRoot {
    pool: Pool,
    index: u16,
}

impl From<&ManifestEntry> for EntryJson {
    fn from(entry: &ManifestEntry) -> Self {
        Self {
            range_version: entry.range_version,
            start_height: entry.start_height,
            end_height: entry.end_height,
            end_hash: entry.end_hash,
            end_tree_sizes: entry.end_tree_sizes,
            sapling_frontier: entry.frontiers.sapling.clone(),
            orchard_frontier: entry.frontiers.orchard.clone(),
            ironwood_frontier: entry.frontiers.ironwood.clone(),
            subtree_roots: entry.subtree_roots.iter().map(SubtreeRootJson::from).collect(),
            blob_digest: entry.blob_digest.0,
            blob_len: entry.blob_len,
            epoch_end: entry.epoch_end,
        }
    }
}

impl TryFrom<EntryJson> for ManifestEntry {
    type Error = InvalidSubtreeRoot;

    fn try_from(json: EntryJson) -> Result<Self, Self::Error> {
        Ok(Self {
            range_version: json.range_version,
            start_height: json.start_height,
            end_height: json.end_height,
            end_hash: json.end_hash,
            end_tree_sizes: json.end_tree_sizes,
            frontiers: Frontiers {
                sapling: json.sapling_frontier,
                orchard: json.orchard_frontier,
                ironwood: json.ironwood_frontier,
            },
            subtree_roots: json
                .subtree_roots
                .into_iter()
                .map(SubtreeRoot::try_from)
                .collect::<Result<_, _>>()?,
            blob_digest: BlobDigest(json.blob_digest),
            blob_len: json.blob_len,
            epoch_end: json.epoch_end,
        })
    }
}

impl SerializeAs<ManifestEntry> for EntryJson {
    fn serialize_as<S: Serializer>(
        entry: &ManifestEntry,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        Self::from(entry).serialize(serializer)
    }
}

impl<'de> DeserializeAs<'de, ManifestEntry> for EntryJson {
    fn deserialize_as<D: Deserializer<'de>>(deserializer: D) -> Result<ManifestEntry, D::Error> {
        Self::deserialize(deserializer)?.try_into().map_err(D::Error::custom)
    }
}

impl From<&SubtreeRoot> for SubtreeRootJson {
    fn from(root: &SubtreeRoot) -> Self {
        let (pool, bytes) = match root.root {
            SubtreeNode::Sapling(node) => (Pool::Sapling, node.to_bytes()),
            SubtreeNode::Orchard(node) => (Pool::Orchard, node.to_repr()),
            SubtreeNode::Ironwood(node) => (Pool::Ironwood, node.to_repr()),
        };
        Self { pool, index: root.index.0, root: bytes }
    }
}

impl TryFrom<SubtreeRootJson> for SubtreeRoot {
    type Error = InvalidSubtreeRoot;

    fn try_from(json: SubtreeRootJson) -> Result<Self, Self::Error> {
        let SubtreeRootJson { pool, index, root } = json;
        pool.subtree_node(root)
            .map(|root| Self { index: NoteCommitmentSubtreeIndex(index), root })
            .ok_or(InvalidSubtreeRoot { pool, index })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;
    use ztreamer_indexer::test_utils::hash;

    use super::*;
    use crate::epoch::synthetic_entries;

    /// A manifest with two entries. The first entry has frontiers and subtree roots in all three
    /// pools.
    fn two_entry_manifest() -> Manifest {
        let add = TreeSizes { sapling: 70_000, orchard: 3, ironwood: 0 };
        let mut entries = synthetic_entries(&[add, add]);
        entries[0].frontiers = Frontiers {
            sapling: vec![0x01, 0xab, 0x00],
            orchard: vec![0xcd; 40],
            ironwood: Vec::new(),
        };
        entries[0].subtree_roots = vec![
            SubtreeRoot {
                index: NoteCommitmentSubtreeIndex(0),
                root: SubtreeNode::Sapling(sapling_crypto::Node::from_bytes([1; 32]).unwrap()),
            },
            SubtreeRoot {
                index: NoteCommitmentSubtreeIndex(1),
                root: SubtreeNode::Orchard(orchard::tree::Node::try_from([2; 32]).unwrap()),
            },
            SubtreeRoot {
                index: NoteCommitmentSubtreeIndex(2),
                root: SubtreeNode::Ironwood(ironwood::tree::Node::try_from([3; 32]).unwrap()),
            },
        ];
        entries[1].frontiers = entries[0].frontiers.clone();
        entries[0].blob_digest = BlobDigest([7; 32]);
        entries[0].blob_len = 121_034_512;
        Manifest::new("Mainnet", hash(0), entries).unwrap()
    }

    /// The fields of [`two_entry_manifest`], to break one rule at a time.
    fn fields() -> ManifestFields {
        two_entry_manifest().0
    }

    fn refused(fields: ManifestFields) -> ManifestError {
        Manifest::try_from(fields).unwrap_err()
    }

    #[test]
    fn manifest_round_trips_through_json() {
        let manifest = two_entry_manifest();
        let json = serde_json::to_string(&manifest).unwrap();

        assert_eq!(Manifest::from_json(&json).unwrap(), manifest);
        assert_eq!(manifest.sealed_through(), 1_999);
    }

    #[test]
    fn json_uses_the_field_names_and_encodings_of_the_plan() {
        let manifest = two_entry_manifest();
        let json: Value = serde_json::to_value(&manifest).unwrap();
        let entry = &json["entries"][0];

        assert_eq!(json["genesis_hash"], manifest.genesis_hash().to_string());
        assert_eq!(entry["end_hash"], manifest.entries()[0].end_hash.to_string());
        assert_eq!(entry["end_tree_sizes"]["sapling"], 70_000);
        assert_eq!(entry["end_tree_sizes"]["orchard"], 3);
        assert_eq!(entry["end_tree_sizes"]["ironwood"], 0);
        assert_eq!(entry["sapling_frontier"], "01ab00");
        assert_eq!(entry["ironwood_frontier"], "");
        assert_eq!(entry["subtree_roots"][0]["pool"], "sapling");
        assert_eq!(entry["subtree_roots"][2]["pool"], "ironwood");
        assert_eq!(entry["subtree_roots"][1]["index"], 1);
        assert_eq!(entry["subtree_roots"][1]["root"], "02".repeat(32));
        assert_eq!(entry["blob_digest"], "07".repeat(32));
        assert_eq!(entry["blob_len"], 121_034_512);
        assert_eq!(entry["epoch_end"], false);
        assert_eq!(json["entries"][1]["epoch_end"], true);
    }

    #[test]
    fn serde_checks_the_rules() {
        let mut json = serde_json::to_value(two_entry_manifest()).unwrap();
        json["sealed_through"] = Value::from(2_999);

        let error = serde_json::from_value::<Manifest>(json).unwrap_err();

        assert!(error.to_string().contains("sealed_through 2999"), "{error}");
    }

    #[test]
    fn a_subtree_root_that_is_not_a_field_element_is_refused() {
        let mut json = serde_json::to_value(two_entry_manifest()).unwrap();
        json["entries"][0]["subtree_roots"][1]["root"] = Value::from("ff".repeat(32));

        let error = Manifest::from_json(&json.to_string()).unwrap_err();

        assert!(matches!(error, ManifestError::Json(_)), "{error:?}");
    }

    #[test]
    fn entries_out_of_order_are_refused() {
        let mut manifest = fields();
        manifest.entries.swap(0, 1);

        let error = refused(manifest.clone());

        assert!(
            matches!(error, ManifestError::Discontiguous { index: 0, start_height: 1_000 }),
            "{error:?}"
        );
    }

    #[test]
    fn a_gap_is_refused() {
        let mut manifest = fields();
        manifest.entries = synthetic_entries(&[TreeSizes::default(); 3]);
        manifest.entries.remove(1);

        let error = refused(manifest.clone());

        assert!(
            matches!(error, ManifestError::Discontiguous { index: 1, start_height: 2_000 }),
            "{error:?}"
        );
    }

    #[test]
    fn a_wrong_range_length_or_a_shrinking_tree_is_refused() {
        let mut manifest = fields();
        manifest.entries[1].end_height += 1;
        let error = refused(manifest.clone());
        assert!(matches!(error, ManifestError::RangeLength { index: 1 }), "{error:?}");

        let mut manifest = fields();
        manifest.entries[1].end_tree_sizes.orchard = 2;
        let error = refused(manifest.clone());
        assert!(matches!(error, ManifestError::TreeSizes { index: 1 }), "{error:?}");
    }

    #[test]
    fn a_wrong_version_or_sealed_through_is_refused() {
        let mut manifest = fields();
        manifest.manifest_version = 2;
        let error = refused(manifest.clone());
        assert!(matches!(error, ManifestError::Version { version: 2 }), "{error:?}");

        let mut manifest = fields();
        manifest.sealed_through = 2_999;
        let error = refused(manifest.clone());
        assert!(
            matches!(
                error,
                ManifestError::SealedThrough { sealed_through: 2_999, last_end: Some(1_999) }
            ),
            "{error:?}"
        );

        manifest.entries.clear();
        let error = refused(manifest.clone());
        assert!(matches!(error, ManifestError::SealedThrough { last_end: None, .. }), "{error:?}");
    }

    #[test]
    fn a_pool_that_becomes_inactive_is_refused() {
        let mut manifest = fields();
        manifest.entries[1].frontiers.orchard.clear();

        let error = refused(manifest.clone());

        assert!(matches!(error, ManifestError::PoolInactive { index: 1 }), "{error:?}");
    }

    #[test]
    fn a_wrong_epoch_end_is_refused() {
        let mut manifest = fields();
        manifest.entries[0].epoch_end = true;
        let error = refused(manifest.clone());
        assert!(
            matches!(error, ManifestError::EpochEnd { index: 0, expected: false }),
            "{error:?}"
        );

        manifest.entries = synthetic_entries(&[TreeSizes::default(); 12]);
        manifest.sealed_through = 11_999;
        manifest.entries[9].epoch_end = false;
        let error = refused(manifest.clone());
        assert!(matches!(error, ManifestError::EpochEnd { index: 9, expected: true }), "{error:?}");

        // The last entry of the manifest always closes an epoch.
        manifest.entries[9].epoch_end = true;
        manifest.entries[11].epoch_end = false;
        let error = refused(manifest.clone());
        assert!(
            matches!(error, ManifestError::EpochEnd { index: 11, expected: true }),
            "{error:?}"
        );
    }
}
