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
use zakura_chain::{block, subtree::NoteCommitmentSubtreeIndex};
use ztreamer_indexer::{
    codec::TreeSizes,
    index::RANGE_SIZE,
    manifest_entry::{BlobDigest, Frontiers, MANIFEST_VERSION, ManifestEntry},
    pool::{NodeBytes, Pool, SubtreeRoot},
};

use crate::epoch::Epochs;

/// A snapshot manifest that obeys the rules in the module docs.
///
/// The fields are private. Each way to get a manifest checks the rules: [`Manifest::new`],
/// [`Manifest::from_contiguous_entries`], [`Manifest::from_json`] and serde. Thus a `Manifest`
/// is always valid.
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
    /// use ztreamer_indexer::manifest_entry::ManifestEntry;
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

    /// The manifest of the `range_meta` entries of an index, for `network` with the genesis
    /// block hash. `entries` are in height order from genesis. The manifest takes the entries
    /// that follow each other from genesis, and stops at the first gap. It sets `epoch_end` on
    /// its last entry, because [`Epochs`] closes an epoch at the last entry of a manifest. The
    /// other entries keep their flag, and [`Manifest::new`] checks it with the epoch rule.
    ///
    /// `ztreamerd export-snapshot` and a peer source make their manifests with this
    /// constructor. Thus an export and a peer give the same manifest for the same rows.
    ///
    /// ```
    /// use zakura_chain::block;
    /// use ztreamer_indexer::manifest_entry::ManifestEntry;
    /// use ztreamer_verify::manifest::{Manifest, ManifestError};
    ///
    /// fn manifest(genesis: block::Hash, rows: Vec<ManifestEntry>) -> Result<Manifest, ManifestError> {
    ///     Manifest::from_contiguous_entries("Mainnet", genesis, rows)
    /// }
    /// ```
    pub fn from_contiguous_entries(
        network: impl Into<String>,
        genesis_hash: block::Hash,
        entries: impl IntoIterator<Item = ManifestEntry>,
    ) -> Result<Self, ManifestError> {
        let mut entries: Vec<_> = entries
            .into_iter()
            .scan(0, |next_start, entry| {
                (entry.start_height == *next_start).then(|| {
                    *next_start = entry.end_height.saturating_add(1);
                    entry
                })
            })
            .collect();
        if let Some(last) = entries.last_mut() {
            last.epoch_end = true;
        }
        Self::new(network, genesis_hash, entries)
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

/// The JSON form of [`Pool`]: its [`Pool::as_str`] name.
#[derive(Deserialize, Serialize)]
#[serde(remote = "Pool", rename_all = "lowercase")]
enum PoolJson {
    Sapling,
    Orchard,
    Ironwood,
}

/// The JSON form of a [`SubtreeRoot`].
#[serde_as]
#[derive(Deserialize, Serialize)]
struct SubtreeRootJson {
    #[serde(with = "PoolJson")]
    pool: Pool,
    index: u16,
    #[serde_as(as = "Hex")]
    root: NodeBytes,
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
        Self { pool: root.root.pool(), index: root.index.0, root: root.root.to_bytes() }
    }
}

impl TryFrom<SubtreeRootJson> for SubtreeRoot {
    type Error = InvalidSubtreeRoot;

    fn try_from(json: SubtreeRootJson) -> Result<Self, Self::Error> {
        let SubtreeRootJson { pool, index, root } = json;
        pool.node(root)
            .map(|root| Self { index: NoteCommitmentSubtreeIndex(index), root })
            .ok_or(InvalidSubtreeRoot { pool, index })
    }
}

#[cfg(test)]
mod tests;
